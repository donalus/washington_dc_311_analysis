;; # Data Preprocessing and Analysis with NOJ and Clay

;; This workbook is based on the podcast episode
;; [Science VS: Gentrification: What's really happening?](https://gimletmedia.com/shows/science-vs/39hzkk).
;; In the podcast episode, NYC Open Data is used to look at the relationship between economic
;; status and 311 complaints. This analysis attempts to replicate that study using data
;; for Washington DC, USA.

;; ## Data Sources:
;; The data sources come from [Open Data DC](https://opendata.dc.gov/).
;; - [ACS 5-Year Economic Characteristics DC Ward](https://opendata.dc.gov/datasets/DCGIS::acs-5-year-economic-characteristics-dc-ward/about)
;; - [311 City Service Requests in 2022](https://opendata.dc.gov/datasets/DCGIS::311-city-service-requests-in-2022/about)

(ns index
  (:require [tablecloth.api :as tc]
            [tech.v3.datatype.datetime :as datetime]
            [scicloj.noj.v1.vis.hanami :as hanami]
            [aerial.hanami.templates :as ht]
            [scicloj.kindly.v4.kind :as kind]
            [clojure.string :as str]))

;; Columns of interest (see datasource for data dictionary):
;; - dp03_0062e - median household income in US dollars
;; - dp03_0063e - mean household income in US dollars
;; - namelsad - Legal/Statistcal Area Description Code - DC is broken into 8 "wards".

(def econ-raw
  (-> "data/ACS_5-Year_Economic_Characteristics_DC_Ward.csv.gz"
      (tc/dataset {:key-fn (fn [c] (keyword (str/lower-case c)))})))
  
(def econ-ds
  (-> econ-raw
      (tc/select-columns [:geoid :namelsad :dp03_0062e :dp03_0063e])
      (tc/rename-columns {:geoid :geoid
                          :namelsad :ward-name
                          :dp03_0062e :median_household_income_dollars
                          :dp03_0063e :mean_household_income_dollars})
      (tc/map-rows (fn [{:keys [geoid]}] {:ward (- geoid 11000)}))
      (tc/drop-columns [:geoid])
      (tc/convert-types :ward :int32)))

(tc/info econ-ds)

econ-ds

;; Colums of interest
;; - servicecodedescription
;; - servicetypecodedescription
;; - ward
;; - adddate
(def calls-raw
  (-> "data/311_City_Service_Requests_in_2022.csv.gz"
      (tc/dataset {:key-fn (fn [c] (keyword (str/lower-case c)))
                   :parse-fn {:adddate [:local-date-time "yyyy/MM/dd HH:mm:ss"]
                              :ward [:int32 :tech.v3.dataset/missing]}})))
(def calls-ds
  (-> calls-raw      
      (tc/select-columns [:ward :adddate :servicecodedescription :servicetypecodedescription :organizationacronym])
      (tc/drop-missing :ward)
      (tc/select-rows (fn [row] (not= (str/lower-case (row :ward)) "null")))
      (tc/convert-types :ward :int32)))

(tc/info calls-ds)

(-> calls-ds
    (tc/unique-by :servicecodedescription)
    (tc/select-rows (fn [row] (let [desc (str/lower-case (row :servicecodedescription))]
                                (or (str/includes? desc "dumping")
                                    (str/includes? desc "trash"))))))

(-> calls-ds
    (tc/unique-by :servicecodedescription)
    (tc/select-rows (fn [row] (let [desc (str/lower-case (row :servicecodedescription))
                                    ]
                                (or (str/includes? desc "parking")
                                    (str/includes? desc "abandoned vehicle"))))))

(-> calls-ds
    (tc/unique-by :servicecodedescription)
    (tc/select-rows (fn [row] (let [desc (str/lower-case (row :servicecodedescription))] (str/includes? desc "dockless")))))

(-> calls-ds
    (tc/map-rows (fn [{:keys [servicecodedescription]}] {:parking_complaint (if (str/includes? (str/lower-case servicecodedescription) "parking")
                                                                              "parking" "not-parking")}))
    (tc/group-by [:ward :parking_complaint])
    (tc/aggregate {:n tc/row-count}) 
    (tc/pivot->wider :parking_complaint :n))

(def comb-ds 
  (-> calls-ds
    (tc/map-rows (fn [{:keys [servicecodedescription]}] {:parking_complaint (if (str/includes? (str/lower-case servicecodedescription) "parking")
                                                                              "parking" "not-parking")}))
    (tc/group-by [:ward :parking_complaint])
    (tc/aggregate {:n tc/row-count}) 
    (tc/pivot->wider :parking_complaint :n) 
      (tc/rename-columns {"parking" :parking
                          "not-parking" :not-parking})
    (tc/inner-join econ-ds :ward)
    (tc/order-by [:ward :parking_complaint])))

comb-ds
    
(-> comb-ds
    (hanami/plot ht/bar-chart
                 {:X "ward"
                  :Y "parking"}))
(-> comb-ds
    (hanami/plot ht/bar-chart
                 {:X "ward"
                  :Y "not-parking"}))

(-> comb-ds 
    (hanami/plot ht/bar-chart
                 {:X "ward"
                  :Y "median_household_income_dollars"}))
