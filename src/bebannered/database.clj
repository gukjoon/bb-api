(ns bebannered.database
  (:import [java.util.logging Logger]
	   [com.google.appengine.api.datastore KeyFactory Key])
  (:require [appengine-magic.services.datastore :as ds]
	    [appengine-magic.services.url-fetch :as http]
	    [clojure.contrib.json :as json]
	    [ring.util.codec :as codec]))


;;ds definitions
(ds/defentity Preference
  [^:key adility_id
  name
  cookie
  parent_preference])

(ds/defentity Vendor
  [^:key adility_id
   url
   name
   description
   category])

(ds/defentity Location
  [^:key combo_id
   location
   vendor
   address])

(ds/defentity Deal
  [^:key adility_id
   title
   vendor
   image
   startdate
   enddate
   revenue
   price
   value
   quantity
   fineprint
   dealarea
   active])

(ds/defentity DealArea
  [^:key adility_name
   full_name])

;;location helper functions
(defn- make-location [lat long]
  (com.google.appengine.api.datastore.GeoPt. lat long))

(defn- get-location [address]
  (let [response (http/fetch (str "http://maps.googleapis.com/maps/api/geocode/json?address=" (codec/url-encode address) "&sensor=false"))
	response-obj (json/read-json (new String (:content response)))
	status (:status response-obj)]
    (if (= status "OK")
      (let [location (:location (:geometry (first (:results response-obj))))]
	(println response-obj)
	;;else null
	(com.google.appengine.api.datastore.GeoPt. (:lat location) (:lng location)))
      nil)))
	

(defn- location-id [vendor locvec]
  (let [lat (first locvec)
	long (second locvec)]
    (str vendor "-" lat long)))

;;checks
(defn- check-dealarea [dealarea-key]
  (let [dealarea (ds/retrieve DealArea dealarea-key)]
    (if dealarea
      dealarea
      ;;fancy name must be done administratively
      (let [dealarea-entity (DealArea. dealarea-key "")]
	(ds/save! dealarea-entity)
	dealarea-entity))))

(defn- check-parent [parent-doc]
  (let [parent-key (Integer/parseInt (:id parent-doc))	
	parent (first (ds/query :kind Preference
			 :filter (= :adility_id parent-key)
			 :limit 1))]
    (if parent
      parent
      (let [parent-entity (ds/new* Preference [parent-key (:title parent-doc) nil nil])]
	(ds/save! parent-entity)
	parent-entity))))
	
(defn- check-category [category-doc parent]
  (let [category-key (Integer/parseInt (:id category-doc))
	category-title (:title category-doc)]
    (if parent
      (let [category (first (ds/query :kind Preference
				      :filter (= :adility_id category-key)
				      :limit 1))]
	(if (and category
		 (= (.getParent (ds/get-key-object category)) (ds/get-key-object parent)))
	  category
	  (let [category-entity
		(ds/new* Preference [category-key category-title nil parent] :parent parent)]
	    (ds/save! category-entity)
	    category-entity)))
      (throw (new Exception (str "No parent for category " category-title))))))

;;We assume that only categories require repair and updates
;;This is probably not true

(defn- check-vendor [vendor-doc category]
  (if category
    (let [vendor-key (Integer/parseInt (:id vendor-doc))
	  vendor (ds/retrieve Vendor vendor-key :parent category)]
      (if vendor
	vendor
	(let [vendor-entity 
	      (ds/new* Vendor [vendor-key (:url vendor-doc) (:title vendor-doc)
			       (ds/as-text (:description vendor-doc)) category] :parent category)]
	  (ds/save! vendor-entity)
	  vendor-entity)))
    (throw (new Exception (str "No category key vendor " (:title vendor-doc))))))

(defn- check-location [location-doc vendor]
  (if vendor
    (let [address (:address location-doc)
	  lat (try
		(Float/parseFloat (:latitude location-doc))
		(catch Exception _
		  nil))
	  long (try
		 (Float/parseFloat (:longitude location-doc))
		 (catch Exception _
		   nil))
	  geo-loc (if (and lat long) (make-location lat long) (get-location address))
	  location-key (location-id (.getId (ds/get-key-object vendor)) (if geo-loc [(.getLatitude geo-loc) (.getLongitude geo-loc)] address))
	  location (ds/retrieve Location location-key :parent vendor)]
      (if location
	(ds/get-key-object location)
	(ds/save!
	 (ds/new* Location [location-key
			    geo-loc vendor (:address location-doc)] :parent vendor))))
    (throw (new Exception (str "No vendor for location " (:address location-doc))))))


(defn- check-deal [deal-doc vendor dealarea]
  (if (not dealarea)
    (println "No dealarea for deal"))
  (if vendor
    (let [deal-key (Integer/parseInt (:id deal-doc))
	  deal (ds/retrieve Deal deal-key :parent vendor)]
      (if deal
	(ds/get-key-object deal)
	(ds/save!
	 (ds/new* Deal [deal-key (:title deal-doc) vendor (:image deal-doc)
			(java.util.Date. (:start-date deal-doc)) (java.util.Date. (:end-date deal-doc)) (Float/parseFloat (:revenue deal-doc))
			(Float/parseFloat (:price deal-doc)) (Float/parseFloat (:value deal-doc)) (Integer/parseInt (:quantity deal-doc))
			(ds/as-text (:fineprint deal-doc)) dealarea false] :parent vendor))))
    (throw (new Exception (str "No vendor for deal " (:title deal-doc))))))


;;public api
(defn add-product [product-map]
  (let [deal-area (check-dealarea (:deal-area product-map))]
    (ds/with-transaction
      (let [parent-cat (if (:parent-category product-map)
			 (check-parent (:parent-category product-map)))
	    category   (if parent-cat
			 (check-category (:category product-map) parent-cat)
			 (check-parent (:category product-map)))
	    vendor     (check-vendor (:vendor product-map) category)]
	(doseq [location (:locations product-map)]
	  (check-location location vendor))
	(check-deal (:deal product-map) vendor deal-area)))))

(def *delete-size* 50)

(defn delete-all [cursor gen]
  nil)