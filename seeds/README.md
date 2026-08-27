# Seeds

`city_catalogue.csv` is the only place cities are defined. It is read twice:

* by the ingester (`pipeline.catalogue.load_cities`) to decide what to request;
* by dbt as a seed (`dbt_project.yml` points `seed-paths` here) to build
  `dim_city`.

Keeping one file avoids the classic bug where the pipeline pulls a city that
the warehouse cannot resolve, which shows up as orphaned facts days later.
