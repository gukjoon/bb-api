(ns bebannered.fetch
  (:use bebannered.constants)
  (:require [appengine-magic.services.task-queues :as tasks]
	    [appengine-magic.services.url-fetch :as http]
	    [appengine-magic.services.datastore :as ds]
	    [appengine-magic.services.mail :as mail]
	    [clojure.zip :as zip]
	    [clojure.xml :as xml]
	    [clojure.contrib.json :as json]
	    [clojure.contrib.zip-filter.xml :as zf]))



(ds/defentity ApiFetch [payload dealarea])

(defn api-fetch [deal-area]
  ;;make sure it's not an error
  (let [response (http/fetch (str *adility-url* "?deal_area=" deal-area)
			     :headers {"Content-Type" "application/xml"
				       "X-Adility-Api-Key" *api-key*
				       "Accept" "application/xml"}
			     :deadline 500)]
    (if response
      (let [entity (ApiFetch. (ds/as-blob (:content response)) deal-area)
	    saved (ds/save! entity)]
	(mail/send (mail/make-message :from "jieren.chen@gmail.com"
				      :to "jieren.chen@gmail.com"
				      :subject "Fetched"
				      :text-body (str (.getId saved))
				      :attachments [(mail/make-attachment "jk.txt" (:content response))]))
	(tasks/add! :url "/split" :queue "api-split"
		    :params {"stored" (str (.getId saved))}))
      (mail/send (mail/make-message :from "jieren.chen@gmail.com"
				    :to "jieren.chen@gmail.com"
				    :subject "fetch nil")))))



(defn split-xml [data-key]
  (let [entity (ds/retrieve ApiFetch (Integer/parseInt data-key))]
    (if (not entity)
      (throw (new Exception "Data-key not found"))
      (let [content (:content (xml/parse (java.io.ByteArrayInputStream. (.getBytes (:payload entity)))))
	    dealarea (:dealarea entity)
	    product-list (map
			  (fn [product]
			    (let [zipped (zip/xml-zip product)
			      ;;category data
				  parent-category {:title (zf/xml1-> zipped :business :category :parent :title zf/text)
						   :id (zf/xml1-> zipped :business :category :parent :id zf/text)}
				  
				  category {:title (zf/xml1-> zipped :business :category :title zf/text)
					    :id (zf/xml1-> zipped :business :category :id zf/text)}
				  
				  ;;vendor data
				  vendor {:id (zf/xml1-> zipped :business :id zf/text)
					  :title (zf/xml1-> zipped :business :title zf/text)
					  :description (zf/xml1-> zipped :business :description zf/text)
					  :url (zf/xml1-> zipped :business :site_url zf/text)}
				  
				  
				  ;;location data
				  locations (map (fn [location]
						   {:latitude (zf/xml1-> location :lat zf/text)
						    :longitude (zf/xml1-> location :lng zf/text)
						    :address (let [addy (zf/xml1-> location :address zf/text)]
							       (if addy
								 addy
							     (zf/xml1-> location :location zf/text)))})
						 (zf/xml-> zipped :business :locations :location))
				  ;;deal data
				  main-deal {:id (zf/xml1-> zipped :id zf/text)
					     :image (zf/xml1-> zipped :image zf/text)
					     :start-date (zf/xml1-> zipped :start_date zf/text)
					     :end-date (zf/xml1-> zipped :end_date zf/text)
					     :revenue (zf/xml1-> zipped :reseller_revenue zf/text)
					     :price (zf/xml1-> zipped :price zf/text)
					     :value (zf/xml1-> zipped :value zf/text)
					     :quantity (zf/xml1-> zipped :quantity zf/text)
					     :title (zf/xml1-> zipped :title zf/text)
					     :fineprint (zf/xml1-> zipped :fineprint zf/text)}]
			      
			      {:deal-area dealarea
			       :category category
			       :parent-category (if (not (= (:id parent-category) ""))
						  parent-category
						  false)
			       :vendor vendor
			       :locations locations
			       :deal main-deal}))
			  content)]
	(doseq [product product-list]
	  (tasks/add! :url "/add" :queue "database-add" :payload (json/json-str product)))
	(ds/delete! entity)))))