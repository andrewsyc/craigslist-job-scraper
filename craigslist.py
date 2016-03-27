from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy import MetaData

from sqlalchemy import Table, Column, Integer, Numeric, String, ForeignKey, text
from sqlalchemy import insert
Base = automap_base()
from bs4 import BeautifulSoup
import urllib2
import os
import datetime
import time
from random import randint

# Time the script starter
start = time.time()

def get_city_listings(url, city, dir):
    # Start timing the speed of this script



    """
    Main page filtering for the selected craigslist page, extract links that will then be followed.
    """
    # - - - - - - - - - - Main Page Links
    directory = os.path.dirname(os.path.realpath(__file__)) + "/" + city + "/"
    if not os.path.exists(directory):
        os.makedirs(directory)


    # proxy_support = urllib2.ProxyHandler({"http":"159.203.245.77:80"})
    # opener = urllib2.build_opener(proxy_support)
    # urllib2.install_opener(opener)
    time.sleep(randint(1,4) + randint(5,8) + randint(8,12))
    array = os.listdir(directory)
    print "This is the url: " + url
    # This was just using a text file from the directory
    # print directory + array[5]
    response = urllib2.urlopen(url)
    # file = open(directory + "text.txt", "r")
    # print file
    soup = BeautifulSoup(response.read(), "lxml")
    # print soup
    # tag = soup.find_all(string=re.compile('<div class='))
    tag = soup.find_all("div", attrs={"class": "content"})

    final_filter = BeautifulSoup(str(tag), "lxml")

    # This further filters out all the links that lead to posts
    links = final_filter.find_all("a", attrs={"class": "hdrlnk"})




    # - - - - - - - - - - Postings scraper

    #This is the link text description for each post
    job_title = []
    # The link to the posting
    job_listings = []
    for i in links:
        print i.get_text()

        value = i['href']

        if value[0] == '/' and value[1] == '/':
            pass
        else:
            job_title.append(i.get_text())
            job_listings.append(i['href'])





    # Gets actual listing (whatever it may be) from the extract URL
    # def get_listing(post_url, city):
    for title, listing in zip(job_title, job_listings):
        # Build the url
        url = "http://" + city + ".craigslist.com" + listing
        print "This is the url to be read: " + url

        time.sleep(randint(1,4))
        
        engine = create_engine('mysql+pymysql://"username_goes_here":"passwordgoeshere'
                               '@104.131.175.187/craigslist')





        duplicate_link = False
        listed_url = url

        check_duplicate = engine.execute("SELECT post_url FROM %s" % dir)


        for i in check_duplicate:
            if url == i['post_url']:
                # print "This has a duplicate url"
                # print i['post_url']
                duplicate_link = True
                break
            else:
                duplicate_link = False




        if not duplicate_link:
            print check_duplicate
            response = urllib2.urlopen(url)
            # print "this is the url we are using: " + post_url
            # print "directory: " + os.path.dirname(os.path.realpath(__file__))
            page = BeautifulSoup(response.read(), "lxml")

            # info on what the job or post is about
            post_body = page.find_all("section", attrs={"id": "postingbody"})

            # pulls out the post text from the content post
            extracted_text = BeautifulSoup(str(post_body), "lxml")
            # print extracted_text.get_text()
            string = extracted_text.get_text()
            string = string.strip('[\\n')
            string = string.strip('\\n]')



            # strips the post entry and is entirely text
            print "This is the url: " + url
            print "This is the title: " + title
            print "This is the descriptino: " + string


            # - - - - - - - - - - All database logic



            # session = Session(engine)
            # Base.prepare(engine, reflect=True)
            # mapped classes are now created with names by default
            # matching that of the table name.






            # s = select(['posts'])



            metadata = MetaData()
            craigslist = Table(dir, metadata,
                Column('post_id', Integer(), primary_key=True),
                Column('post_date', String(20)),
                Column('post_name', String(255), index=True),
                Column('post_url', String(255)),
                Column('post_content', String(20000))
                )
            metadata.create_all(engine)

            # Make sure to add items here that were parsed
            ins = insert(craigslist).values(
                # post_id is auto inserted
                post_date=datetime.date.today(),
                post_name=title,
                post_url=url,
                post_content=string,
            )

            # insert into database the parsed logic
            engine.execute(ins)









    # - - - - - - - - - - -


# prepend = ["ames"]
prepend = ["auburn", "birmingham", "dothan", "shoals", "gadsden", "mobile", "montgomery", "tuscaloosa", "huntsville", "fairbanks", "anchorage", "flagstaff", "phoenix", "prescott",
"tucson", "yuma", "fayetteville", "jonesboro", "texarkana", "bakersfield", "chico", "merced", "modesto", "redding", "sacramento", "stockton", "susanville", "boulder", "denver", "pueblo",
"hartford", "delaware", "washington", "losangeles", "monterey", "orangecounty", "palmsprings", "sandiego", "sfbay", "slo", "santabarbara", "santamaria", "siskiyou", "ventura", "visalia", "yubasutter",
"humboldt", "imperial", "inlandempire", "rockies", "westslope", "newlondon", "newhaven", "showlow", "sierravista", "fortsmith", "littlerock", "goldcountry", "hanford", "nwct", "mohave", "juneau",
"cosprings", "eastco", "fortcollins", "kenai", "gainesville", "jacksonville", "lakeland", "ocala", "orlando", "pensacola", "tallahassee", "panamacity", "sarasota", "miami", "spacecoast", "staugustine", "tampa", "treasure",
"miami", "fortmyers", "cfl", "lakecity", "okaloosa", "keys", "daytona", "albany", "athens", "atlanta", "augusta", "brunswick", "columbus", "statesboro", "valdosta", "macon", "nwga", "savannah",
"hawaii", "boise", "lasalle", "mattoon", "peoria", "rockford", "carbondale", "decatur", "springfieldil", "quincy", "chicago", "bn", "twinfalls", "lewiston", "eastidaho", "bloomington", "evansville", "indianapolis", "kokomo", "muncie",
"tippecanoe", "richmondin", "southbend", "terrehaute", "fortwayne", "ames", "dubuque", "iowacity", "masoncity", "quadcities", "siouxcity", "ottumwa", "waterloo", "fortdodge", "desmoines", "cedarrapids", "lawrence",
"salina", "seks", "swks", "topeka", "wichita", "lexington", "houma", "lafayette", "monroe", "shreveport", "maine", "annapolis", "baltimore", "frederick", "louisville", "owensboro", "westky", "batonrouge", "cenla", "lakecharles",
"neworleans", "easternshore", "smd", "westmd", "eastky", "albany", "binghamton", "buffalo", "catskills", "chautauqua", "newyork", "oneonta", "plattsburgh", "longisland", "potsdam", "rochester", "syracuse", "twintiers", "utica", "watertown",
"elmira", "fingerlakes", "glensfalls", "hudsonvalley", "ithaca", "jxn", "kalamazoo", "lansing", "monroemi", "muskegon", "nmi", "porthuron", "saginaw", "swmi", "thumb", "battlecreek", "centralmich", "detroit", "flint", "grandrapids",
"holland", "up", "annarbor", "laredo", "mcallen", "odessa", "sanangelo", "sanantonio", "charlottesville", "danville", "fredericksburg", "harrisonburg", "richmond", "roanoke", "winchester", "charleston", "morgantown", "swva",
"martinsburg", "huntington", "wheeling", "parkersburg", "swv", "wv", "blacksburg", "lynchburg", "norfolk", "sanmarcos", "corpuschristi",
"dallas", "nacogdoches", "delrio", "elpaso", "galveston", "houston", "killeen", "bigbend", "texoma", "easttexas", "amarillo", "austin", "beaumont", "brownsville", "collegestation", "victoriatx", "wichitafalls", "abilene"]




# append = "search/web"
append = ['web',]

for dir in append:
    for city in prepend:
        print city
        time.sleep(5.5)
        # ensure_dir(city)
        url = "http://{}.craigslist.com/search/{}".format(city, dir)
        print "This is the directory: " + dir
        get_city_listings(url, city, dir)


end = time.time()
print(end - start)





