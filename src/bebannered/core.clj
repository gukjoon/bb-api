(ns bebannered.core
  (:use compojure.core)
  (:use bebannered.constants)
  (:require [bebannered.fetch :as fetch]
	    [bebannered.database :as db]
	    [appengine-magic.services.mail :as mail]
	    [appengine-magic.core :as ae]
	    [appengine-magic.services.task-queues :as tasks]
	    [clojure.contrib.duck-streams :as duck]
	    [clojure.contrib.json :as json]))

(defmacro try-wrapper [& body]
  (try
    `(do
       ~@body
       {:status 200 :headers {"Content-Type" "text/plain"} :body "Success!"})
    (catch Exception e
      {:status 500 :headers {"Content-Type" "text/plain"} :body "Failure!"})))

;;public gets for dev purposes
(defn- cron-handler []
  (try-wrapper
   (doseq [area (keys *deal-areas*)]
     (tasks/add! :url "/fetch" :queue "api-fetch" :params {"dealarea" area}))))

(defn- forcedelete-handler []
  (try-wrapper
   (tasks/add! :url "/delete" :queue "ds-delete" :params {"gen" "0"})))


(defn- fetch-handler [dealarea]
  (try-wrapper (fetch/api-fetch dealarea)))

(defn- split-handler [xml-bytes]
  (try-wrapper (fetch/split-xml xml-bytes)))

(defn- add-handler [json-body]
  (let [json-str (duck/slurp* json-body)]
    #_(mail/send (mail/make-message :from "jieren.chen@gmail.com"
				  :to "jieren.chen@gmail.com"
				  :subject "Fetched"
				  :text-body json-str))
    (try-wrapper (db/add-product (json/read-json json-str)))))

(defn- delete-handler [cursor gen]
  (try-wrapper (db/delete-all cursor gen)))


(defroutes bb-api-app-handler
  (GET "/force" [] (cron-handler))
  (GET "/delete" [] (forcedelete-handler))
  ;;split up cron by city/region?
  (POST "/cron" [] (cron-handler))
  
  (POST "/fetch" [dealarea] (fetch-handler dealarea))
  ;;how to do body in compojure
  (POST "/split" [stored] (split-handler stored))
  (POST "/add" {body :body} (add-handler body))
  (POST "/delete" [cursor gen] (delete-handler cursor (Integer/parseInt gen))))




;;MAGIC! DO NOT TOUCH
(ae/def-appengine-app
  bebannered-app
  #'bb-api-app-handler)