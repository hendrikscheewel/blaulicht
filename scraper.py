import requests
import numpy as np
import time
import os
import sqlite3
from requests.exceptions import Timeout

urls_name = 'urls.db'
db_name = 'data.db'
root_url = "https://www.presseportal.de/blaulicht/pm/"

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0'}
s = requests.Session()

def create_db_if_not_exists():
    """
    create_db_if_not_exists() -> no output
    
    Creates a database to store scraping results
    and a queue if these files do not exist yet.
    
    """
    
    # Create data.db if not in current directory
    if db_name in os.listdir():
        pass
    else:
        print('Database created: '+db_name)
        conn = sqlite3.connect(db_name) # connect to database
        c = conn.cursor()
        c.execute("""CREATE TABLE data(station_id INT, 
                                       article_id INT, 
                                       html TEXT, 
                                       UNIQUE(station_id, article_id))""")
        conn.commit()
        conn.close()
       
        
        
def load_queue(max_size):
    """
    load_queue(max_size = int) -> list
    
    Loads a queue (parameters for url composition) of maximum length max_size.
    
    """

    # Load n = max_size items from url database
    conn = sqlite3.connect(urls_name)
    c = conn.cursor()
    
    c.execute('''SELECT * FROM data WHERE article_id IN (SELECT article_id FROM data ORDER BY RANDOM() LIMIT {max_size})'''.format(max_size = max_size))
    queue = c.fetchall()
    conn.close()
        
    # Pass queue
    return(queue)
        
    
    
    
def write_to_db(records):
    """
    write_to_db(records = list) -> no output
    
    Receives a list of records and writes them to the sql database.
    Closes connection when done.
    
    """
    
    # Write to database
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.executemany('''INSERT OR IGNORE INTO 
                     data(station_id, article_id, html) VALUES(?,?,?)''',records)
    conn.commit()
    conn.close()
    
    
    
def count_remaining():
    """
    count_remaining() -> str
    
    Prints number of remaining urls.
    
    """
    conn = sqlite3.connect(urls_name)
    c = conn.cursor()
    c.execute('''SELECT COUNT(*) FROM data''')
    count = int(c.fetchall()[0][0])
    conn.close()  
    return(count)
    
    
    
    
def remove_from_urls(records):
    """
    remove_from_urls(records=list)
    
    Removes those items from urls database that have been successfully scraped.
    
    """
    
    # compose remove_list
    remove_list = []
    for rec in records:
        remove_list += [(rec[0],rec[1])]

    # delete from urls
    conn = sqlite3.connect(urls_name)
    c = conn.cursor()
    c.executemany('''DELETE FROM data WHERE station_id = ? AND article_id = ?''',remove_list)
    conn.commit()
    conn.close()  
    

    
    
def scrape(station_id, article_id):
    """
    scrape(station_id=int, article_id=int) -> str
    
    Composes url from station_id and article_in and returns page html as string.
    
    """
    
    url = root_url + str(station_id) + "/" + str(article_id) # compose url
    print(url+' '*50,end='\r')
    
    response = s.get(url, headers = headers, timeout=10) # get data from website
    html = response.text
    return(html)




def main(max_size=100):
    """
    main(max_size=int) -> no output

    Main scraping function. 
    
    """
    
    create_db_if_not_exists()
    queue = load_queue(max_size) # fill first queue
    
    while (len(queue) > 0):
        records = [] # empty records
        start_time = time.time()
        
        while len(queue) > 0:
            # select random item from queue
            station_id, article_id = queue[np.random.choice(range(len(queue)))]
            
            try:
                # scrape that item
                html = scrape(station_id, article_id)

                # add scraped item to records
                records += [(station_id, article_id, html)]

                # remove the item upon success
                queue.remove((station_id, article_id))
            
            except Timeout:
                print("Timeout error.")
                
            except:
                print("Uncaught error.")
                break


        write_to_db(records)
        remove_from_urls(records)
        stop_time = time.time()
        print("Items remaining: " +str(count_remaining()))
        if len(records) > 0:
            records_per_hour = (60**2)*((stop_time - start_time)/len(records))
            print("Scraping speed: " + str(round(records_per_hour,2)) + " (records/h)")
            print("Hours to completion: " +str(round(count_remaining()/records_per_hour)))
        else:
            print("Nothing scraped.")
        queue = load_queue(max_size) # refill queue