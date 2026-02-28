;;; pool.clj -- HikariCP connection pooling for ejc-sql.

;;; Copyright © 2024 - djnesmith

;;; This program is free software; you can redistribute it and/or modify
;;; it under the terms of the GNU General Public License as published by
;;; the Free Software Foundation; either version 2, or (at your option)
;;; any later version.
;;;
;;; This program is distributed in the hope that it will be useful,
;;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;; GNU General Public License for more details.
;;;
;;; You should have received a copy of the GNU General Public License
;;; along with this program; if not, write to the Free Software Foundation,
;;; Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.  */

(ns ejc-sql.pool
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
           [java.sql DriverManager]
           [javax.sql DataSource]
           [java.io PrintWriter]
           [java.util.logging Logger]))

(def pool-registry
  "Maps db-identity-key -> HikariDataSource"
  (atom {}))

(def proxy-lock
  "Global lock for serializing SOCKS proxy System property manipulation."
  (Object.))

(defn db-identity-key
  "Returns a vector uniquely identifying a database connection."
  [db]
  (mapv db [:subprotocol :subname :user :connection-uri :dbtype :dbname :host :port]))

(defn db->jdbc-url
  "Converts a db-spec map to a JDBC URL string."
  [db]
  (or (:connection-uri db)
      (str "jdbc:" (:subprotocol db) ":" (:subname db))))

(defn- proxy-aware-datasource
  "Creates a DataSource that sets SOCKS proxy properties before connecting.
  Acquires proxy-lock to serialize proxy property manipulation across threads."
  [db]
  (let [url (db->jdbc-url db)
        user (:user db)
        password (:password db)
        proxy-host (:proxy-host db)
        proxy-port (:proxy-port db)
        login-timeout (atom 0)
        log-writer (atom nil)]
    (reify DataSource
      (getConnection [_]
        (locking proxy-lock
          (if (and proxy-host proxy-port)
            (do
              (System/setProperty "socksProxyHost" proxy-host)
              (System/setProperty "socksProxyPort" proxy-port))
            (do
              (System/clearProperty "socksProxyHost")
              (System/clearProperty "socksProxyPort")))
          (if (and user password)
            (DriverManager/getConnection url user password)
            (DriverManager/getConnection url))))
      (getConnection [_ user password]
        (locking proxy-lock
          (if (and proxy-host proxy-port)
            (do
              (System/setProperty "socksProxyHost" proxy-host)
              (System/setProperty "socksProxyPort" proxy-port))
            (do
              (System/clearProperty "socksProxyHost")
              (System/clearProperty "socksProxyPort")))
          (DriverManager/getConnection url user password)))
      (getLoginTimeout [_] @login-timeout)
      (setLoginTimeout [_ seconds] (reset! login-timeout seconds))
      (getLogWriter [_] @log-writer)
      (setLogWriter [_ writer] (reset! log-writer writer))
      (getParentLogger [_] (Logger/getLogger Logger/GLOBAL_LOGGER_NAME))
      (isWrapperFor [_ iface] false)
      (unwrap [_ iface] (throw (java.sql.SQLException. "Not a wrapper"))))))

(defn make-pool
  "Creates a HikariDataSource for the given db-spec."
  [db]
  (let [config (doto (HikariConfig.)
                 (.setDataSource (proxy-aware-datasource db))
                 (.setMaximumPoolSize 3)
                 (.setMinimumIdle 1)
                 (.setIdleTimeout 300000)      ; 5 minutes
                 (.setMaxLifetime 600000)       ; 10 minutes
                 (.setConnectionTimeout 30000)  ; 30 seconds
                 (.setPoolName (str "ejc-" (hash (db-identity-key db)))))]
    (HikariDataSource. config)))

(defn get-pooled-db
  "Returns {:datasource hikari-ds} for use with clojure.java.jdbc.
  Creates the pool on first call for a given db identity."
  [db]
  (let [key (db-identity-key db)]
    (if-let [ds (get @pool-registry key)]
      {:datasource ds}
      (let [ds (make-pool db)]
        (swap! pool-registry assoc key ds)
        {:datasource ds}))))

(defn close-pool
  "Closes the pool for a specific db-spec."
  [db]
  (let [key (db-identity-key db)]
    (when-let [ds (get @pool-registry key)]
      (.close ds)
      (swap! pool-registry dissoc key))))

(defn close-all-pools
  "Closes all connection pools."
  []
  (doseq [[_ ds] @pool-registry]
    (.close ds))
  (reset! pool-registry {}))

(defn pool-status
  "Returns pool metrics for debugging."
  []
  (into {}
        (map (fn [[key ds]]
               [key {:total (.getTotalConnections (.getHikariPoolMXBean ds))
                     :active (.getActiveConnections (.getHikariPoolMXBean ds))
                     :idle (.getIdleConnections (.getHikariPoolMXBean ds))
                     :waiting (.getThreadsAwaitingConnection (.getHikariPoolMXBean ds))}])
             @pool-registry)))
