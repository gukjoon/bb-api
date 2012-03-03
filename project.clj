(defproject bebannered "0.1"
  :description "backend job to pull data from Adility"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
		 [compojure "0.5.3"]
		 [ring/ring-servlet "0.3.1"]
		 [appengine-magic "0.3.2"]]
  :dev-dependencies [[appengine-magic "0.3.2"]]
  :aot [bebannered.app_servlet
	bebannered.core
	bebannered.fetch
	bebannered.database]
  :main bebannered)