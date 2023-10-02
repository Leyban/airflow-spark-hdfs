from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from datetime import datetime
from datetime import timedelta
import oss2
import time

dag = DAG('extract_bongda24h_live', description='extract bongda24h live',
        schedule_interval='*/10 * * * *', start_date=datetime(2020, 8, 30), catchup=False)


mongo_hook = MongoHook(conn_id='sports_conn_id')
auth = oss2.Auth('LTAI4G72yYuCRT1LVQwZryAf', 'Wynm9Ourtmgv0SOzDCOrWPq9BAcxoB')
bucket_name = "fb88sports"
path_endpoint_bucket = "http://" + bucket_name + \
    "." + "oss-cn-hangzhou.aliyuncs.com"
bucket = oss2.Bucket(auth, "http://oss-cn-hangzhou.aliyuncs.com", bucket_name)
live_score_path = "https://bongda24h.vn/LiveScore/AjaxLivescore?date="
schedules_path = "https://bongda24h.vn/Schedules/AjaxSchedules?date="
standings_path = "https://bongda24h.vn/bang-xep-hang.html"
bongda24h_path = "https://bongda24h.vn"


schedules_left_menu_path = "https://bongda24h.vn/bong-da-anh/ket-qua-1.html"
ajax_schedules_by_league = "https://bongda24h.vn/Schedules/AjaxSchedulesByLeague?"

def get_id(path):
    id = ""
    if path != None:
        x = path["href"].split("-")
        if len(x) > 1:
            spl = x[len(x)-1].split(".")
            if len(spl) > 1:
                id = spl[0]
    return id

def get_id2(path):
    id = ""
    if path != None:
        x = path.split("-")
        if len(x) > 1:
            spl = x[len(x)-1].split(".")
            if len(spl) > 1:
                id = spl[0]
    return id

def get_events_live_score(soup, is_schedules, is_live_score, is_today):
    events = []
    for sibling in soup.next_siblings:
        event = {}
        if ('table-header' in sibling["class"]):
            break
        
        if ('football-match' in sibling["class"]):
            continue
            
        if 'football-header' in sibling["class"]:
            event["title"] = sibling.find("h3").get_text()
            event["league_id"] = get_id(sibling.find("a"))
            matches = []
            for frow in sibling.next_siblings:
                if 'football-header' in frow["class"]:
                    break
                if 'football-match' in frow["class"]:
                    if ((frow.find(class_="club1") != None) & (frow.find(class_="club2") != None)):
                        match = []
                        id = get_id(frow.find("a", attrs={'class': 'btn-f-more'}))
                        match.append(id)
                        match.append(frow.find(class_="date").text.strip())
                        match.append(frow.find(class_="club1").text.strip())
                        match.append(get_link_img(get_name_img(frow.find(
                                class_="club1").find("img")["src"], frow.find(class_="club1").text.strip())))
                        match.append(frow.find(class_="club2").text.strip())
                        match.append(get_link_img(get_name_img(frow.find(
                                class_="club2").find("img")["src"], frow.find(class_="club2").text.strip())))
                        match.append(frow.find(class_="soccer-scores").text.strip())
                        is_live = 0
                        if frow.find(class_="fa-spin") != None:
                            is_live = 1
                        match.append(is_live)

                        if ((is_live_score == True) | (is_today == True & is_live == 1)):
                            detail = get_detail_match(bongda24h_path + frow.find("a", attrs={'class': 'btn-f-more'})["href"], id, get_id(sibling.find("a")))
                            save_db_match_detail(detail, id)
                        matches.append(match)

            event["matches"] = matches

            events.append(event)

    return events


def get_date_list(is_today):
    count = 9
    i = -1
    
    if is_today == True:
        count = 1
        i = 0
    
    date_list = []
    while i < count:
        date = (datetime.now() + timedelta(days=i)).strftime("%d-%m-%Y")
        date_list.append(date)
        i = i+1
    return date_list


def get_live_score_schedules(path, date_list, is_schedules, is_live_score, is_today):
    league_id = "0"

    data_list = []

    for date in date_list:
        r = requests.get(path+date+"&leagueId="+league_id)
        soup = bs(r.content)
        data = {}
        data["date"] = date
        tt = soup.find_all(class_='table-header')
        i = 0
        if len(tt) == 0:
            continue
        if len(tt) > 1:
            i = 1
        data["title"] = tt[i].get_text()
        data["events"] = get_events_live_score(tt[i], is_schedules, is_live_score, is_today)
        data_list.append(data)

    return data_list


def get_name_img(path, name):
    if 'MacDinh' not in path:
        x = path.split(".")
        if len(x) > 0:
            return name.replace(" ", "-") + "." + x[len(x)-1]
    return "MacDinh.svg"


def get_link_img(name):
    #x = path_endpoint_bucket + "/" + name
    x = name
    return x


def get_standing(soup):
    data_table = []
    table = soup.find('table', attrs={'class': 'table-bxh'})
    if table != None:
        table_body = table.find('tbody')

        rows = table_body.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) > 0:
                cs = []
                for index, ele in enumerate(cols):
                    if index == 1:
                        cs.append(get_link_img(get_name_img(ele.find("img")["src"], ele.text.strip())))
                    cs.append(ele.text.strip())
                data_table.append(cs)

    return data_table

def get_detail_match_action(live_action):
    uls = live_action.find_all('ul')
    act = {}
    home_team_actions = []
    for sibling in uls[0].find_all("li"):
        action = [b for b in sibling.stripped_strings]
        if(len(action) > 0):
            if ("yellow_card" in sibling.find("img")["src"]):
                action.append("yellow_card")
            if  ("goal" in sibling.find("img")["src"]):
                action.append("goal")
            if ("red_card" in sibling.find("img")["src"]):
                action.append("red_card")
            if ("yellow_red_card" in sibling.find("img")["src"]):
                action.append("yellow_red_card")   
            
            if len(action) > 0:
                home_team_actions.append(action)
    
    if len(home_team_actions) > 0:
        act["home_team"] = home_team_actions
        
    away_team_actions = []
    for sibling in uls[1].find_all("li"):
        action = [b for b in sibling.stripped_strings]
        if(len(action) > 0):
            if ("yellow_card" in sibling.find("img")["src"]):
                action.append("yellow_card")
            if  ("goal" in sibling.find("img")["src"]):
                action.append("goal")
            if ("red_card" in sibling.find("img")["src"]):
                action.append("red_card")
            if ("yellow_red_card" in sibling.find("img")["src"]):
                action.append("yellow_red_card")
            if len(action) > 0:
                away_team_actions.append(action)
                
    if len(away_team_actions) > 0:
        act["away_team"] = away_team_actions
    return act


def get_detail_match(path, id, league_id):

    r = requests.get(path)
    soup = bs(r.content)

    detail = {}
    detail["id"] = id
    detail["title"] = soup.find('h1',attrs={'class': 'the-article-title'}).text.strip()
    detail["score"] = soup.find(class_='c2-result').text.strip()
    detail["home_team"] = soup.find(class_='c1-result').find(class_="name-tie").text.strip()
    detail["home_team_logo"] = get_link_img(get_name_img(soup.find(class_='c1-result').find("img")["src"], detail["home_team"]))
    detail["away_team"] = soup.find(class_='c3-result').find(class_="name-tie").text.strip()
    detail["away_team_logo"] = get_link_img(get_name_img(soup.find(class_='c3-result').find("img")["src"], detail["away_team"]))
    
    
    #live_table = soup.find('table', attrs={'class': 'tablelive'})
    #trs = live_table.find_all('tr')
    #if len(trs) > 1:
    #   detail["status"] = trs[0].text.strip()
    #   detail["time"] = trs[1].find(class_="td-r").text.strip()
    #   detail["league_id"] = league_id
    #   detail["league_name"] = trs[1].find(class_="td-l").text.strip()

    
    live_action = soup.find("div", attrs={'tabs-data': 'tab-db'})
    action_tab = get_detail_match_action(live_action)
    if len(action_tab) > 0:
        detail["action_tab"] = action_tab

    result_livescore = soup.find("div", attrs={'tabs-data': 'tab-ls'}) 
    live_scores_tab = get_detail_match_result(result_livescore)
    if len(live_scores_tab) > 0:
        detail["live_scores_tab"] = live_scores_tab
    
    
    return detail


def get_detail_match_result(result_livescore): 
    result = []
    for sibling in result_livescore.find_all(class_="fix-doidau1"):
        data = {}
        data["title"] = sibling.get_text()
        data["events"] = get_events_live_score(sibling, False, False, False)
        result.append(data)
    return result



def instance_extract_live():
    start = time.time()
    date_list = get_date_list(True)
    live_score_data = get_live_score_schedules(live_score_path, date_list, False, False, True)
    save_db_live(live_score_data)
    end = time.time()
    print("extract live score : ", end - start)


def save_db_live(data):
    mongo_hook.delete_many("live", {'date':datetime.now().strftime("%d-%m-%Y")})
    mongo_hook.insert_many("live", data)
    return True


def save_db_match_detail(data, id):
    mongo_hook.delete_many("match_details", {'id': id})
    mongo_hook.insert_one("match_details", data)
    return True


rebate_operator = PythonOperator(
    task_id='extract_bongda24h_live', python_callable=instance_extract_live, dag=dag)