(ns metabase.sync.analyze.fingerprint.sample
  "Analysis sub-step that fetches a sample of values for a given Field, which is used to generate a fingerprint for it.
   Currently this is dumb and just fetches a contiguous sequence of values, but in the future we plan to make this
   more sophisticated and have different types of samples for different Fields."
  (:require [metabase.driver :as driver]
            [metabase.models
             [database :refer [Database]]
             [field :as field]
             [table :refer [Table]]]
            [metabase.sync.interface :as i]
            [schema.core :as s]
            [toucan.db :as db]))

(s/defn ^:always-validate basic-table-sample :- (s/maybe i/ValuesSample)
  "Procure a sequence of non-nil values, up to `max-sync-lazy-seq-results` (10,000 at the time of this writing), for use
   in the various tests above. Maybe return `nil` if no values are available."
  [table :- i/TableInstance, fields :- [i/FieldInstance]]
  ;; TODO - we should make `->driver` a method so we can pass things like Fields into it
  (let [db-id    (:db_id table)
        driver   (driver/->driver db-id)
        database (Database db-id)]
    (driver/sync-in-context driver database
      (fn []
        (->> (driver/table-values-sample driver table fields)
             (take driver/max-sync-lazy-seq-results)
             seq)))))

(s/defn ^:always-validate ^:deprecated basic-sample :- (s/maybe i/ValuesSample)
  "Procure a sequence of non-nil values, up to `max-sync-lazy-seq-results` (10,000 at the time of this writing), for use
   in the various tests above. Maybe return `nil` if no values are available.

   DEPRECATED: We probably don't need this function anymore since we have `basic-table-sample` instead."
  [field :- i/FieldInstance]
  (let [table-sample (basic-table-sample (field/table field) [field])
        field-sample (map (keyword (:name field)) table-sample)]
    (seq (filter (complement nil?) field-sample))))
