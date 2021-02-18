import requests
import numpy as np
import time
import os
import sqlite3

urls_name = 'urls.db'
db_name = 'data.db'
queue_name = 'queued.db'
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
    count = str(c.fetchall()[0][0])
    conn.close()  
    print(count+' '*50)
    
    
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
    
    response = s.get(url, headers = headers) # get data from website
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
        
        while len(queue) > 0:
            # select random item from queue and remove it
            station_id, article_id = queue[np.random.choice(range(len(queue)))]
            queue.remove((station_id, article_id))
            
            # scrape that item
            html = scrape(station_id, article_id)

            # add scraped item to records
            records += [(station_id, article_id, html)]

        write_to_db(records)
        remove_from_urls(records)
        count_remaining()
        queue = load_queue(max_size) # refill queue