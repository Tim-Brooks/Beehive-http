(ns beehive-http.core
  (:require [beehive.future :as f])
  (:import (com.ning.http.client AsyncHttpClient RequestBuilder)
           (com.ning.http.client.providers.netty.response NettyResponse)
           (net.uncontended.precipice ServiceProperties RejectedActionException)
           (net.uncontended.precipice_implementations.asynchttp HttpAsyncService)))

(set! *warn-on-reflection* true)

(defn async-http-client []
  (AsyncHttpClient.))

(defn request [url]
  (.build (doto (RequestBuilder.)
            (.setUrl ^String url)
            (.setRequestTimeout 500))))

(defn response [^NettyResponse response]
  {:status (.getStatusCode response)
   :headers (.getHeaders response)
   :body (.getResponseBodyAsStream response)})

(defn service [name]
  (let [properties (ServiceProperties.)]
    (HttpAsyncService. name properties (async-http-client))))

(defn execute [service request]
  (try
    (f/->CLJResilientFuture
      (.submitRequest ^HttpAsyncService service request))
    (catch RejectedActionException e
      (f/->CLJRejectedFuture (.reason e)))))