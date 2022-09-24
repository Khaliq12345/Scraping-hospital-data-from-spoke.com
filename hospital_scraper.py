
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
import time
import requests

urls = []
start_time = time.time()
names = []
categories = []
locations = []
phones  = []
emails = []
websites = []
linkedins = []
num = 0

for x in range(1, 100):
    url = f'https://www.spoke.com/search?page={x}&q=Hospitals&type=company&utf8=%E2%9C%93'
    urls.append(url)

async def parse(soup):
    cards = soup.find_all('div',{'class':'sr-details'})
    for card in cards:
        link = card.find('a')['href']
        link = 'https://www.spoke.com' + link
        with requests.get(link) as r:
            sup = BeautifulSoup(r.text, 'lxml')
            try:
                name = sup.find('h1', {'itemprop':'name'}).text.strip()
                names.append(name)
            except:
                names.append('')
            profile = sup.find_all('div',{'class':'sub-profile'})
            try:
                category = profile[0].text.strip()
                categories.append(category)
            except:
                categories.append('')
            try:
                loc = profile[1].text.strip()
                locations.append(loc)
            except:
                locations.append('')
            try:
                phone = profile[10].text.strip()
                phones.append(phone)
            except:
                phones.append('')
            try:
                email = profile[11].text.strip()
                emails.append(email)
            except:
                emails.append('')
            try:
                website = profile[12].find('a')['href']
                websites.append(website)
            except:
                websites.append('')
            try:
                linkedin = profile[16].find('a')['href']
                linkedins.append(linkedin)
            except:
                linkedins.append('')



async def get_data(session, url, throttler):
    async with throttler:
        async with session.get(url) as r:
            global num
            num = num + 1
            print(num)
            html = await r.text()
            soup = BeautifulSoup(html, 'lxml')
            page = await parse(soup)
            return page

async def get_all(session, urls,throttler):
    tasks = []
    for url in urls:
        task = asyncio.create_task(get_data(session, url, throttler))
        tasks.append(task)
    result = await asyncio.gather(*tasks)
    return result

async def main(urls):
    async with aiohttp.ClientSession() as session:
        data = await get_all(session, urls, throttler)
        return data

if __name__ == '__main__':
    throttler = asyncio.Semaphore(5)
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    result = asyncio.run(main(urls))
    print(result)

df = pd.DataFrame({
    'Name':names, 
    'Category':categories,
    'Loaction':locations,
    'Phone':phones,
    'Email':emails,
    'Website':websites,
    'Linkedin':linkedins
})

df.to_csv('spoke_hospital.csv')
print(df)

print("--- %s seconds ---" % (time.time() - start_time))


