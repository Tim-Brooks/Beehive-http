(ns beehive-http.core
  (:import (net.uncontended.precipice_implementations.asynchttp HttpAsyncService)
           (net.uncontended.precipice ServiceProperties)
           (com.ning.http.client AsyncHttpClient)))

(defn client []
  (AsyncHttpClient.))

(defn service [name]
  (let [properties (ServiceProperties.)]
    (HttpAsyncService. name properties (client))))
