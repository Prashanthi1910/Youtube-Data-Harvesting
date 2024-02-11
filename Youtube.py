from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
from datetime import datetime 
import streamlit as st
import time
from googleapiclient.errors import HttpError

# API key Connection

def api_connect():
    api_id="AIzaSyDyJ6U-X982nJTaTnu82aW5B84E_0MK0i0"
    api_service_name = "youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=api_id) 


    return youtube


youtube=api_connect()

#get channel information
def get_channel_info(channel_id):
    request = youtube.channels().list(
                part = "snippet,contentDetails,statistics",
                id= channel_id
            )

    response1 = request.execute()



    for i in range(0,len(response1['items'])):
        data=dict(channel_Name=response1['items'][i]["snippet"]["title"],
                channel_Id=response1['items'][i]["id"],
                channel_des=response1['items'][i]["snippet"]["description"],
                subscriber=response1['items'][i]["statistics"]["subscriberCount"],
                viewCount=response1['items'][i]["statistics"]["viewCount"],
                video_count=response1['items'][i]["statistics"]["videoCount"],
                playlist_id=response1['items'][i]["contentDetails"]["relatedPlaylists"]["uploads"])
        return data
    
#get playlist ids

def get_playlist_info(channel_id):
    All_data =[]
    next_page_token = None
    next_page = True
    while True:
        request= youtube.playlists().list(
            part = "snippet,contentDetails",
            channelId =channel_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()
        for item in response['items']:
            data = dict(playlists_Id = item['id'],
                        Title = item['snippet']['title'],
                        channel_Id = item['snippet']['channelId'],
                        channel_Name = item['snippet']['channelTitle'],
                        playlist_published = item['snippet']['publishedAt'],
                        Video_count = item['contentDetails']['itemCount']) 
            All_data.append(data)
        next_page_token= response.get('nextPageToken')
        if next_page_token is None:
            next_page= False
        
        return All_data
    
#get video_ids
def get_channel_videos(channel_id):
    video_ids=[]
    response2= youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()


    playlist_id=response2["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token= None

    while True:

        response2=youtube.playlistItems().list(
                                                part='snippet',
                                                playlistId=playlist_id,
                                                maxResults=50,
                                                pageToken=next_page_token).execute()


        for i in range (len(response2['items'])):
            video_ids.append(response2['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response2.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


#st. write (get_video_ids('UC9xghV-TcBwGvK-aEMhpt5w'))


def get_video_info(Video_Ids):

    video_data=[]
    for video_id in Video_Ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()

    
        for item in response['items']:
            data=dict(channel_Name=item['snippet']['channelTitle'],
                    channel_Id=item['snippet']['channelId'],
                    video_Id=item['id'],
                    title=item['snippet']['title'],
                    tags=item['snippet'].get('tags'),
                    thumbnail=item['snippet']['thumbnails']['default']['url'],
                    description=item['snippet']['description'],
                    published_date=item['snippet']['publishedAt'],
                    duration=item['contentDetails']['duration'],
                    views=item['statistics']['viewCount'],
                    Likes=item['statistics'].get('likeCount'),
                    comments=item['snippet'].get('commentCount'),
                    favourite_count=item['statistics']['favoriteCount'],
                    definiton=item['contentDetails']['definition'],
                    captions= item['contentDetails']['caption'])
            video_data.append(data)
    return video_data

# get comment information
def get_comment_info(video_ids):

    comment_information= []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 50
                )
            response4 = request.execute()
            for item in response4['items']:
                data = dict(comment_Id= item['snippet']['topLevelComment']['id'],
                            video_Id =item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_Text =item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                comment_information.append(data)
    except:
        pass
    return comment_information



#Mongo DB

client=pymongo.MongoClient("mongodb://localhost:27017/")
db= client["Youtube_Data_Harvesting"]


def channel_details(channel_id):
    ch_details =get_channel_info(channel_id)
    pl_details =get_playlist_info(channel_id)
    vi_ids =get_channel_videos(channel_id)
    vi_details =get_video_info(vi_ids)
    comm_details =get_comment_info(vi_ids)


    
    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                    "video_information":vi_details,"comment_Details":comm_details})
    
    return "upload completed successfully"


#Table creation for channels playlists,videos,comments

def channels_table():
    mydb= mysql.connector.connect(host='localhost',
                                user='root',
                                password = 'Prashanthi@19',
                                database ='Youtube_Data_Harvesting' )
    cursor=mydb.cursor()


    '''drop_query = "drop table channels"
    cursor.execute(drop_query)'''

    try:

        sql = "create table channels(channel_Name varchar(100),channel_Id varchar(150) primary key,channel_des text, subsciber_count bigint, viewCount bigint, video_count int, playlist_id varchar(150) )"
        cursor.execute(sql)
    except:
        st.write("Channels table already created")


    ch_list=[]
    db = client["Youtube_Data_Harvesting"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)



    for index, row in df.iterrows():
        insert_query = '''insert into channels values(%s,%s,%s,%s,%s,%s,%s) '''

        values =(row['channel_Name'],
                row['channel_Id'],
                row['channel_des'],
                row['subscriber'],
                row['viewCount'],
                row['video_count'],
                row['playlist_id'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            st.write("values inserted")


def playlist_table():
    mydb= mysql.connector.connect(host='localhost',
                                user='root',
                                password = 'Prashanthi@19',
                                database ='Youtube_Data_Harvesting' )
    cursor=mydb.cursor()

    '''drop_query = "drop table playlists"
    cursor.execute(drop_query)'''

    try:
        sql = '''create table playlists(playlists_Id  varchar(100) primary key,
                                        Title  varchar(150),
                                        channel_Id  varchar(100),
                                        channel_Name varchar(100), 
                                        playlist_published timestamp,
                                        Video_count int)'''
        cursor.execute(sql)
    except:
        st.write("Playlists tables are already inserted")

    
    db = client["Youtube_Data_Harvesting"]
    coll1 = db["channel_details"]
    pl_list=[]

    for pl_data in coll1.find({},{"_id":0, "playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        insert_query1 = "INSERT INTO playlists (playlists_Id, Title, channel_Id, channel_Name, playlist_published, Video_count) VALUES (%s, %s, %s, %s, STR_TO_DATE(%s, '%Y-%m-%dT%H:%i:%sZ'), %s)"

        values = (row['playlists_Id'],
                row['Title'],
                row['channel_Id'],
                row['channel_Name'],
                row['playlist_published'],
                row['Video_count'])
        
        try:
            cursor.execute(insert_query1, values)
            mydb.commit()
        except:
            st.write("Playlist values are already inserted")

        

def videos_table():
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Prashanthi@19',
        database='Youtube_Data_Harvesting'
    )

    cursor = mydb.cursor()

    # Drop the table if it exists
    '''drop_query = "DROP TABLE IF EXISTS videos"
    cursor.execute(drop_query)'''

    try:

        # Create the table
        sql = '''CREATE TABLE videos (
            channel_Name VARCHAR(100),
            channel_Id VARCHAR(150),
            video_Id VARCHAR(100) PRIMARY KEY,
            title VARCHAR(100), 
            tags TEXT,
            thumbnail VARCHAR(200),
            description TEXT,
            published_date TIMESTAMP,
            duration INT,
            views INT,
            Likes INT,
            comments INT,
            favourite_count INT,
            definiton VARCHAR(500),
            captions VARCHAR(50)
        )'''
        cursor.execute(sql)

    except:
        st.write("Videos Table already created")

    vi_list = []
    # Assuming 'client' and 'db' are defined somewhere else in your code
    # and 'channel_details' is a MongoDB collection
    db = client["Youtube_Data_Harvesting"]
    coll1 = db["channel_details"]

    for pl_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(pl_data["video_information"])):
            vi_list.append(pl_data["video_information"][i])

    df2 = pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        insert_query3 = '''VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        VALUES = (
            row['channel_Name'],
            row['channel_Id'],
            row['video_Id'],
            row['title'],
            ','.join(row['tags']) if isinstance(row['tags'], list) else row['tags'],
            row['thumbnail'],
            row['description'],
            row['published_date'],
            row['duration'],
            row['views'],
            row.get('Likes',0),
            row['comments'],
            row['favourite_count'],
            row['definiton'],
            row['captions'])

        try:
            cursor.execute(insert_query3, VALUES)
            mydb.commit()
            print("Values inserted successfully.")
        except:
            st.write("videos values already inserted")


def comments_table():
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Prashanthi@19',
        database='Youtube_Data_Harvesting'
    )
    cursor = mydb.cursor()

    #Drop the table if needed
    drop_query = "DROP TABLE IF EXISTS comments"
    cursor.execute(drop_query)

    try:
        # Create the comments table
        sql = '''CREATE TABLE IF NOT EXISTS comments (
            comment_Id VARCHAR(100) PRIMARY KEY,
            video_Id VARCHAR(150),
            comment_Text TEXT,
            comment_Author VARCHAR(100),
            comment_Published timestamp
        )'''
        cursor.execute(sql)

    except:
        st.write("comments tables are already inserted")

    db = client["Youtube_Data_Harvesting"]
    coll1 = db["channel_details"]

    cmm_list = []
    for pl_data in coll1.find({}, {"_id": 0, "comment_Details": 1}):
        for i in range(len(pl_data["comment_Details"])):
            cmm_list.append(pl_data["comment_Details"][i])

    df3 = pd.DataFrame(cmm_list)

    for index, row in df3.iterrows():
        insert_query4 = '''INSERT INTO comments VALUES (%s, %s, %s, %s, %s)'''

        values = (
            row['comment_Id'],
            row['video_Id'],
            row['comment_Text'],
            row['comment_Author'],
            row['comment_Published']
        )

        try:
            cursor.execute(insert_query4, values)
            mydb.commit()
        except:
            st.write("This comments are already inserted")

    # Close the MySQL connection
  



def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"


def show_channel():
    ch_list=[]
    db = client["Youtube_Data_Harvesting"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)

    return channels_table


def show_playlist():
    pl_list=[]
    db = client["Youtube_Data_Harvesting"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({},{"_id":0, "playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    playlist_table= st.dataframe(pl_list)

    return playlist_table

def show_video():
    vi_list=[]
    db = client["Youtube_Data_Harvesting"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({},{"_id":0, "video_information":1}):
        for i in range(len(pl_data["video_information"])):
            vi_list.append(pl_data["video_information"][i])
    videos_table = st.dataframe(vi_list)

    return videos_table


def show_comment():
    cmm_list = []
    db = client["Youtube_Data_Harvesting"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "comment_Details": 1}):
        for i in range(len(pl_data["comment_Details"])):
            cmm_list.append(pl_data["comment_Details"][i])

    comments_table = st.dataframe(cmm_list)
    return comments_table


# streamlit 

with st.sidebar:
    st.title("YOUTUBE DATA HARVESTING")
    st.header("skill take away")
    st.caption("Python Scriptiting")
    st.caption("Mongo DB")





channel_id = st.text_input("Channel_Id")

if st.button("Collect and store data"):
    ch_ids = []
    db = client["Youtube_Data_Harvesting"]
    coll1 = db["channel_details"]
    
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        if "channel_information" in ch_data and "Channel_Id" in ch_data["channel_information"]:
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    
    if channel_id in ch_ids:
        st.success("Channel details of the given channel id: " + channel_id + " already exist")
    else:
        output = channel_details(channel_id)
        st.success(output)

if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)

show_table = st.radio("SELECT THE TABLE FOR VIEW", (":green[channels]", ":orange[playlists]", ":red[videos]", ":blue[comments]"))


if show_table ==":green[channels]":
    show_channel()
elif show_table== ":orange[playlists]":
    show_playlist()
elif show_table == ":red[videos]":
    show_video()
elif show_table == ":blue[comments]":
    show_comment()

#sql connection

mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Prashanthi@19',
        database='Youtube_Data_Harvesting'
    )
cursor = mydb.cursor()


question= st.selectbox(
    "Please Select Your Questions",(
    '1. All the Videos and the channel Name',
    '2. Channels with the most number of videos',
    '3. 10 most viewed videos',
    '4. Comments in each video',
    '5. Videos with the highest likes',
    '6. likes of all videos',
    '7. views of each channel',
    '8. videos published in the year 2022',
    '9. average duration of all the videos in each channel',
    '10. videos with highest number of comments'))


if question == '1. All the Videos and the channel Name':
    query1 = "select title as Videos, channel_name as ChannelName from videos;"
    cursor.execute(query1)
    t1= cursor.fetchall()
    st.write(pd.DataFrame(t1, columns= ["Video Title", "Channel Name"]))
elif question == '2. Channels with the most number of videos':
    query2 = " select channel_Name as ChannelName, video_count as No_Videos from channels order by video_count desc;"
    cursor.execute(query2)
    t2= cursor.fetchall()
    st.write(pd.DataFrame(t2, columns = ["Channel Name", "No of Videos"]))
elif question == '3. 10 most viewed videos':
    query3= ''' select views as views, channel_Name as ChannelName, title as VideoTitle from videos
                    where views is not null order by views desc limit 10;'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns= ["views", "channel name", "video title"]))
elif question == '4. Comments in each video':
    query4 = "select comments as No_comments, title as VideoTitle from videos where comments is not null;"
    cursor.execute(query4)
    t4= cursor.fetchall()
    st.write(pd.DataFrame(t4, columns= ["No of Comments", "Video Title"]))
elif question == '5. Videos with the highest likes':
    query5 = ''' select title as VideoTitle, channel_Name as ChannelName, Likes as Likecount from videos
                    where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns= ["Video Title", "Channel Name", "Like count"]))
elif question == '6. likes of all videos':
    query6 = ''' select Likes as likecount, title as VideoTitle from videos;'''
    cursor.execute(query6)
    t6= cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count", "video title"]))
elif question == '7. views of each channel':
    query7 = "select channel_Name as ChannelName, viewCount as Channelviews from channels;"
    cursor.execute(query7)
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns= ["channel name", "total views"]))
elif question == '8. videos published in the year 2022':
    query8 = ''' select title as Video_Title, published_date as VideoRealease, channel_Name as ChannelName from videos
                    where extract(year from published_date)= 2022;'''
    cursor.execute(query8)
    t8 = cursor.fetchall()
    st.write(pd.DataFrame(t8, columns=["Name", "Video Published On", "ChannelName"]))

elif question =='9. average duration of all the videos in each channel':
    query9 = "select channel_Name as ChannelName, AVG(duration) as average_duration  from videos GROUP By channel_Name;"
    cursor.execute(query9)
    t9= cursor.fetchall()
    t9 = st.write(pd.DataFrame(t9, columns = ["Channel Title", "Average Duration"]))
   
elif question == '10. videos with highest number of comments':
    query10 = ''' select Title as VideoTitle, channel_Name as ChannelName, comments as Comments from videos
                    where comments is not null order by comments desc;'''
    cursor.execute(query10)
    t10= cursor.fetchall()
    st.write(pd.DataFrame(t10,columns=['Video Title', 'Channel Name', 'No of comments']))


