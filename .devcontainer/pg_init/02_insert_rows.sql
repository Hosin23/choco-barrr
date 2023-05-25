copy choco(company, bean_origin_specfic, ref, review_date, cocoa_percent, company_location, rating, bean_type, bean_origin_broad)
from '/docker-entrypoint-initdb.d/seed_data/flavors_of_cacao.csv'
delimiter ','
csv header;