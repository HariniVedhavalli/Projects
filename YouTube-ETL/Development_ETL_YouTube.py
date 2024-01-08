#Importing required packages
import streamlit as st
import pymongo
import pyodbc
import googleapiclient.discovery
import pprint
import isodate
import pandas as pd
import googleapiclient
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --------------Extracting data from youtube using API---------------
from googleapiclient.discovery import build
api_key = 'AIzaSyD2qQoSXEDYWMn1R-YSxayJRauQJi1t350'
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)
# Extracting Channel Details

def get_channel_data(Channel_id):
    request = youtube.channels().list( id=Channel_id,
                                       part='snippet,statistics,contentDetails'
                                     )
    reponse = request.execute()
    c_data={
        'channel_id': reponse['items'][0]['id'],
        'channel_name': reponse['items'][0]['snippet']['title'],
        'channel_description':reponse['items'][0]['snippet']['description'],
        'playList_id': reponse['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        'published_at': reponse['items'][0]['snippet']['publishedAt'],
        'channel_views': reponse['items'][0]['statistics']['viewCount'],
        'video_count': reponse['items'][0]['statistics']['videoCount'],
        'subscriber_count': reponse['items'][0]['statistics']['subscriberCount'],
        }
    return(c_data)
    
# Exracting the video IDs
#Getting all the video IDs respective to Playlist_id

def get_video_ids(playlist_id):

    video_ids = []
    next_page=None

    while True:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults = 50,
            pageToken=next_page
        )
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])    
        next_page = response.get('nextPageToken')
        if next_page is None:
            break
    return video_ids
    
#Extracting video and comment details
def get_video_comment_details(video_ids):

    video_comments_details={}

    for i in video_ids:
        request = youtube.videos().list(
                  id=i,
                  part='snippet,contentDetails,statistics')
        response=request.execute()
                #Comments

        try:
            request1 = youtube.commentThreads().list(
                   part="snippet, replies",
                   maxResults=10,
                   videoId=i)
    
            response1 = request1.execute()
            video_comments={}
            if len(response1['items'])>0:
                for j in range(0,len(response1['items'])):
                    comments={
                            'comment_id': response1['items'][j]['snippet']['topLevelComment']['id'],  
                            'video_id':i,
                            'comment_author': response1['items'][j]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'comment_text':response1['items'][j]['snippet']['topLevelComment']['snippet']['textOriginal'],
                            'comment_published_at':response1['items'][j]['snippet']['topLevelComment']['snippet']['publishedAt'],
                            'comment_likes_count':response1['items'][j]['snippet']['topLevelComment']['snippet']['likeCount']
                            }
                    video_comments['comment_id'+str(j+1)]=comments
        except:
               comments={
                            'comment_id': None,  
                            'video_id':i,
                            'comment_author': None,
                            'comment_text':None,
                            'comment_published_at':None,
                            'comment_likes_count':None
                        }
               video_comments['comment_']=comments
    #video
        video_details={
            'video_id': i,
            'video_name':response['items'][0]['snippet']['title'],
            'video_desc':response['items'][0]['snippet']['description'],
            'tags':','.join(response['items'][0]['snippet'].get('tags',['NA'])),
            'published_at':response['items'][0]['snippet']['publishedAt'],
            'view_count':response['items'][0]['statistics']['viewCount'],
            'like_count':response['items'][0]['statistics']['likeCount'],
            'favorite_count':response['items'][0]['statistics']['favoriteCount'],
            'commentcount':response['items'][0]['statistics'].get('commentCount',0),
            'duration':response['items'][0]['contentDetails']['duration'],
            'thumbnail':{'default_thumbnail_url' :response['items'][0]['snippet']['thumbnails'].get('default', {}).get('url', 'NA'),
                        'high_thumbnail_url' : response['items'][0]['snippet']['thumbnails'].get('high', {}).get('url', 'NA'),
                        'maxres_thumbnail_url' : response['items'][0]['snippet']['thumbnails'].get('maxres', {}).get('url', 'NA'),
                        'medium_thumbnail_url' : response['items'][0]['snippet']['thumbnails'].get('medium', {}).get('url', 'NA'),
                        'standard_thumbnail_url' :response['items'][0]['snippet']['thumbnails'].get('standard', {}).get('url', 'NA')},
            'caption_status':response['items'][0]['contentDetails']['caption'],
            'comments': video_comments }

        video_comments_details['Video_id' + '_' + str(video_ids.index(i) + 1)]=video_details

    return video_comments_details
#-------------------------------------------------------------
# receiving channel id from user
def GetChannelIds(channelID,status):
    channel_info = []
    status.write('Connecting and Extracting the data from YouTube : ' + channelID)
    channel = {}

    try:
        channel['channel_id'] = channelID
        channel['channel_details']=get_channel_data(channelID)
        playlist_id = channel['channel_details']['playList_id']
        video_ids= get_video_ids(playlist_id)
        channel['video_details']=get_video_comment_details(video_ids)
        channel_info.append(channel)

        return channel_info
    
    except Exception as e:
        status.write("Unable to scrap data from channel : " + channelID)
        print("Unable to scrap data from channel : " + channelID)
        return -1

# --------------Data Insertion / Updation into MongoDB ---------------

def MigratingDataToMongoDb(table,data):
    flag = 0
    query = {'channel_details.channel_id':data[0]['channel_details']['channel_id']}
    project = {'channel_details.channel_id':1,'_id':0}
    res = table.find(query,project)

    for i in res:
        table.update_one({'channel_id':i['channel_details']['channel_id']},
                            {"$set":{'channel_details':data[0]['channel_details'],
                                    'video_details':data[0]['video_details']}})
        flag = 1

    if flag == 0:
        table.insert_one(data[0])
        flag = 1

# --------------Data Insertion / Updation into SSMS ---------------

#-----------------------------------------------------------------
# parsing duration
def convert_to_HH_MM_SS(duration):
    duration = duration[2:]  # Removing 'PT' from the start
    time_values = {'H': 0, 'M': 0, 'S': 0}
    current_value=''
    for char in duration:
        if char.isdigit():
            current_value += char
        elif char.isalpha():
            time_values[char] = int(current_value)
            current_value = ''

    hours = str(time_values['H']).zfill(2) if time_values['H'] > 0 else '00'
    minutes = str(time_values['M']).zfill(2) if time_values['M'] > 0 else '00'
    seconds = str(time_values['S']).zfill(2) if time_values['S'] > 0 else '00'
    return f"{hours}:{minutes}:{seconds}"

# parsing date from string to datetime format
def parse_date(published_date):
    return isodate.parse_datetime(published_date)
#-----------------------------------------------------------------------
def AppendChannelDetails(mycursor,channel_details,isPresent):
    if (isPresent):
        query = '''UPDATE channel SET channel_name=?, 
                                channel_views=?, 
                                channel_description=?, 
                                channel_subscibers=? 
                                where channel_id=?'''
        data = (
            channel_details['channel_name'],
            int(channel_details['channel_views']),
            channel_details['channel_description'],
            int(channel_details['subscriber_count']),
            channel_details['channel_id'],
        )
    else:
        query = 'INSERT INTO channel values (?,?,?,?,?)'
        data = (
            channel_details['channel_id'],
            channel_details['channel_name'],
            int(channel_details['channel_views']),
            channel_details['channel_description'],
            int(channel_details['subscriber_count'])
        )
    mycursor.execute(query,data)
    
# inserting / updating video and comment details in sql
def AppendVideoAndCommentDetails(mycursor,
                                 video_details,
                                 playlist_id,
                                 channel_id,
                                 isPresent):
    if (isPresent):
        query = """DELETE FROM Comment where video_id IN 
            (SELECT video_id from Video as v WHERE v.channel_id=?)"""
        data = (channel_id,)
        mycursor.execute(query,data)
        query = 'DELETE FROM Video where channel_id=?'
        data = (channel_id,)
        mycursor.execute(query,data)
        
    for video in video_details.keys():
        duration = convert_to_HH_MM_SS(video_details[video]['duration'])
        pub_date = parse_date(video_details[video]['published_at'])
        like_count = video_details[video]['like_count']
        com_count = video_details[video]['commentcount']
        query = """ INSERT INTO Video values
                    (?,?,?,?,?,?,?,?,?,?,?,?,?) """
        data = (
            video_details[video]['video_id'],
            playlist_id,
            video_details[video]['video_name'],
            video_details[video]['video_desc'],
            pub_date,
            int(video_details[video]['view_count']),
            int( 0 if like_count is None else like_count),
            int(video_details[video]['favorite_count']),
            int(0 if com_count is None else com_count),
            duration,
            video_details[video]['thumbnail']['default_thumbnail_url'],
            video_details[video]['caption_status'],
            channel_id
        )
        mycursor.execute(query,data)

        if(video_details[video]['comments'] is None):
            continue
            
        for comment in video_details[video]['comments'].keys():

            com_pub_date = \
            parse_date(video_details[video]['comments'][comment]['comment_published_at'])

            query = 'INSERT INTO Comment values (?,?,?,?,?)'
            data = (
                video_details[video]['comments'][comment]['comment_id'],
                video_details[video]['video_id'],
                video_details[video]['comments'][comment]['comment_text'],
                video_details[video]['comments'][comment]['comment_author'],
                com_pub_date,
            )
            mycursor.execute(query,data)

#---------------------------------------------
def MigratingDataToSQL(mycursor,channel_data):
    query = 'select count(*) from Channel where channel_id=?'
    data = (channel_data[0]['channel_id'],)
    mycursor.execute(query,data)
    isPresent = 0
    for i in mycursor:
        isPresent = i[0]
    AppendChannelDetails (mycursor,
                        channel_data[0]['channel_details'],
                        isPresent)
    AppendVideoAndCommentDetails(mycursor,
                                  channel_data[0]['video_details'],
                                  channel_data[0]['channel_details']['playList_id'],
                                  channel_data[0]['channel_id'],
                                  isPresent)

#---------------------------------------
# -------------- Query Execution in SQL ---------------

def ExecuteQuery(mycursor,query):
    mycursor.execute(query)
    res=mycursor.fetchall()
    field_headings=[i[0] for i in mycursor.description]
    return pd.DataFrame(res,columns=field_headings)
# -------------- Main Method ---------------

def main():
    
    # Connecting to API
    api_key = 'AIzaSyD2qQoSXEDYWMn1R-YSxayJRauQJi1t350'
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)


    # connecting with mongo DB
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["YouTube_Raw"]
    table = db["YT_Channels"]

    # Create a connection string

    conn_str ='DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=YouTube_DataWarehouse;UID=SQLDeveloper;PWD=harini'

    mydb = pyodbc.connect(conn_str)

    # Create a cursor object
    mycursor = mydb.cursor()



    # streamlit page setup
    st.set_page_config(
        page_title = "Youtube DataWarehousing",

        page_icon = \
            ":black_right_pointing_triangle_with_double_vertical_bar:",

        initial_sidebar_state = "collapsed"
        )
    tab1, tab2, tab3, tab4 = st.tabs(["Home", 
                                            "About", 
                                            "Query", 
                                            "View"])

    # streamlit portal design
    # "Home" tab population
    with tab1:
        st.header("Youtube DataWarehousing")
        channel_id_value = st.text_input('Enter the Channel ID to Scrap')
        if(channel_id_value != ""):
            if(st.button('Scrap')):
                status = st.empty()
                channel_data = GetChannelIds(channel_id_value,status)
                if(channel_data != -1):
                    status.write(":green[Scrapped Data Successfully]")
                    with st.expander("View JSON Format"):
                        st.json(channel_data)
                    MigratingDataToMongoDb(table,channel_data)
                    MigratingDataToSQL(mycursor,channel_data)
                    mydb.commit()

    # "About" tab population
    with tab2:
        st.header("About")
        st.write("""Want to extract details of any youtube channel and analyse
                 the data in it - Here you go... This website aims at
                 scrapping the youtube channel details with channel ID.
                 Also, it provides the room for querying, viewing and
                 the channel data scrapped.\n""")

    # "Query" tab population   
    with tab3:
        st.header("Query")
        map_query_options = {
            '1) What are the names of all the videos and their corresponding\
                  channels?':
            'SELECT v.video_name,c.channel_name FROM video as v INNER JOIN \
                channel as c ON c.channel_id=v.channel_id',

            '2) Which channels have the most number of videos, \
                    and how many videos do they have?':
            """SELECT c.channel_name,COUNT(v.video_id) as video_count \
                FROM video as v INNER JOIN channel as c \
                ON c.channel_id=v.channel_id GROUP BY c.channel_name \
                ORDER BY video_count DESC LIMIT 1""",

            '3) What are the top 10 most viewed videos and their respective\
                  channels':
            """SELECT v.video_name,v.view_count,c.channel_name \
                FROM video as v INNER JOIN channel as c \
                ON c.channel_id=v.channel_id ORDER BY v.view_count DESC \
                LIMIT 10""",

            '4) How many comments were made on each video, \
                    and what are their corresponding video names?':
            'SELECT v.video_name,v.comment_count FROM video as v',

            '5) Which videos have the highest number of likes, \
                and what are their corresponding channel names?':
            """SELECT v.video_name,v.like_count,c.channel_name \
                FROM video as v INNER JOIN channel as c ON c.channel_id=v.channel_id
                ORDER BY v.like_count DESC LIMIT 10""",

            """6) What is the total number of likes and dislikes for each video, 
            and what are their corresponding video names?""":
            'SELECT v.video_name,v.like_count  FROM video as v',

            """7) What is the total number of views for each channel, 
                and what are their corresponding channel names?""":
            """SELECT c.channel_name,SUM(v.view_count) as total_views \
                FROM channel as c INNER JOIN video as v ON \
                c.channel_id=v.channel_id GROUP BY c.channel_name""",

            '8) What are the names of all the channels that have published \
                videos in the year 2022?':
            """SELECT c.channel_name, COUNT(YEAR(v.published_date)) as \
                count_of_videos_uploaded FROM channel as c INNER JOIN video \
                as v ON v.channel_id=c.channel_id WHERE YEAR(v.published_date)="2022" \
                GROUP BY c.channel_name""",

            """9) What is the average duration of all videos in each channel, 
                and what are their corresponding channel names?""":
            """SELECT c.channel_name, AVG(v.duration) as average_duration \
                FROM channel as c INNER JOIN video as v ON c.channel_id=v.channel_id \
                GROUP BY c.channel_name""",

            """10) Which videos have the highest number of comments, 
                and what are their corresponding channel names?""":
            """SELECT c.channel_name,v.video_name,v.comment_count \
                FROM video as v INNER JOIN channel as c ON c.channel_id=v.channel_id \
                ORDER BY v.comment_count DESC LIMIT 10"""
        }
        sql_query = st.selectbox('Select query from drop down menu...', 
                                 map_query_options.keys())
        query = map_query_options[sql_query]
        df = ExecuteQuery(mycursor,query)
        if not df.empty:
            st.dataframe(df)
        else:
            st.write("No Results Found !")

    # "View" tab population
    with tab4:
        st.header("View")
        tables = st.multiselect('Select table(s) to display',
                                ['Channels', 'Videos', 'Comments'])
        if(tables):
            if(st.button('View Table')):
                for table in tables:
                    if table == 'Channels':
                        query = 'SELECT * FROM channel'
                        st.write(":blue[Channel Details]")
                        st.dataframe(ExecuteQuery(mycursor,query))
                    if table == 'Videos':
                        query = 'SELECT * FROM video'
                        st.write(":blue[Video Details]")
                        st.dataframe(ExecuteQuery(mycursor,query))
                    if table == 'Comments':
                        query = 'SELECT * FROM comment'
                        st.write(":blue[Comment Details]")
                        st.dataframe(ExecuteQuery(mycursor,query))


    # adding CSS to tabs
    css = '''
        <style>
            .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
             font-size:1rem;
            }
        </style>
        '''
    st.markdown(css, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
