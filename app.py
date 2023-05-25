import math
import psycopg2
from flask import Flask, render_template, request
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(
    "host=db dbname=postgres user=postgres password=postgres",
    cursor_factory=RealDictCursor)
app = Flask(__name__)


@app.route('/')
def hello_world():
    return "Hello, World!"

def safe_parse_int(value):
    try:
        return int(value)
    except:
        return None

def is_int(value):
    try:
        int(value)
        return True
    except:
        return False

@app.route('/choco')
def render():
    company = request.args.get("company", "")
    company_location = request.args.get("company_location", "")
    origin_specfic = request.args.get("bean_origin_specfic", "")
    origin_broad = request.args.get("origin_broad", "")
    date_gte = request.args.get("date_gte", 2005)
    date_lte = request.args.get("date_lte", 2023)
    cocoa_gte = request.args.get("cocoa_gte", 0)
    cocoa_lte = request.args.get("cocoa_lte", 100)
    result_per_page = request.args.get("result_per_page", 20)
    sort_by = request.args.get("sort_by", "rating")
    sort_dir = request.args.get("sort_dir", "asc")
    page = request.args.get("page", 1)

    if date_gte is None or not is_int(date_gte):
        date_gte = 2005

    if date_lte is None or not is_int(date_lte):
        date_lte = 2023

    if cocoa_gte is None or not is_int(cocoa_gte):
        cocoa_gte = 0

    if cocoa_lte is None or not is_int(cocoa_lte):
        cocoa_lte = 100

    if result_per_page is None or not is_int(result_per_page) or safe_parse_int(result_per_page) <= 0:
        result_per_page = 20
    
    if page is None or not is_int(page):
        page = 1

    limit = result_per_page
    offset = safe_parse_int(result_per_page) * (safe_parse_int(page) - 1)

    SORT_COLUMNS = {"cocoa_percent", "review_date", "rating"}
    SORT_DIR = {"asc", "desc"}

    if sort_by is None or sort_by not in SORT_COLUMNS:
        sort_by = "rating"
    
    if sort_dir is None or sort_dir not in SORT_DIR:
        sort_dir = "asc"

    from_where_clause = """
        from choco
        where lower(company) like %(company)s
        and lower(company_location) like %(company_location)s
        and lower(bean_origin_specfic) like %(origin_specfic)s
        and lower(bean_origin_broad) like %(origin_broad)s
        and cast(review_date as int) between %(date_gte)s and %(date_lte)s
        and cast(substring(cocoa_percent, 1 , length(cocoa_percent) - 1) as double precision) between %(cocoa_gte)s and %(cocoa_lte)s
    """
    params = {
        "company": f"%{company}%",
        "company_location": f"%{company_location}%",
        "origin_specfic": f"%{origin_specfic}%",
        "origin_broad": f"%{origin_broad}%",
        "date_gte": date_gte,
        "date_lte": date_lte,
        "cocoa_gte": cocoa_gte,
        "cocoa_lte": cocoa_lte,
        "result_per_page": result_per_page,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "limit": limit,
        "offset": offset,
        "page": page
    }

    def get_sort_dir(col):
        if col == sort_by:
            return "desc" if sort_dir == "asc" else "asc"
        else:
            return "asc"

    with conn.cursor() as cur:
        cur.execute(f"""select
                            company,
                            bean_origin_specfic,
                            cast(review_date as int),
                            cast(substring(cocoa_percent, 1 , length(cocoa_percent) - 1) as double precision) cocoa_percent,
                            company_location,
                            cast(rating as double precision),
                            bean_type,
                            bean_origin_broad
                        {from_where_clause}
                        order by {sort_by} {sort_dir}
                        limit %(limit)s offset %(offset)s""", params)
        results = list(cur.fetchall())

        cur.execute(f"select count(*) as count {from_where_clause}", params)
        count = cur.fetchone()["count"]
        
        page_num = math.ceil(count / safe_parse_int(result_per_page))

        input_params = {
            "company": company,
            "company_location": company_location,
            "origin_specfic": origin_specfic,
            "origin_broad": origin_broad,
            "date_gte": date_gte,
            "date_lte": date_lte,
            "cocoa_gte": cocoa_gte,
            "cocoa_lte": cocoa_lte,
            "result_per_page": result_per_page,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "limit": limit,
            "offset": offset,
            "page": page
        }

        return render_template("choco.html",
                               params=request.args,
                               input_params=input_params,
                               result_count=count,
                               page_num=page_num,
                               get_sort_dir=get_sort_dir,
                               chocos=results)