(ns bebannered.app_servlet
  (:gen-class :extends javax.servlet.http.HttpServlet)
  (:use bebannered.core)
  (:use [appengine-magic.servlet :only [make-servlet-service-method]]))


(defn -service [this request response]
  ((make-servlet-service-method bebannered-app) this request response))
