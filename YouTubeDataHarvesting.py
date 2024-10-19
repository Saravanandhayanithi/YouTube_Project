# Libraries

from googleapiclient.discovery import build
import pymongo
import mysql.connector 
import pandas as pd
import re
import plotly.express as px
import streamlit as st

# YouTube apikey Connection 

def api_connect():
  
  api_key="AIzaSyD7_64tRIAwzwFfs3VW0FaxWmWXvrIn4QU"

  api_service_name = "youtube"
  api_version = "v3"

  youtube =build (api_service_name, api_version, developerKey=api_key)

  return youtube

youtube=api_connect()

# Mongodb Connection

client = pymongo.MongoClient("mongodb+srv://saravanan:261194@cluster0.dz6tyue.mongodb.net/?retryWrites=true&w=majority")
db=client['YouTube_Data']

# MySql Connection

mydb = mysql.connector.connect( host = 'localhost', user = 'root', database = 'youtube_data', password = '', port = '3306')
mycursor = mydb.cursor(buffered = True)
mycursor.execute('USE youtube_data')

# Channel Details

def get_channel_info(channel_id):
        
        request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id
        )
        response = request.execute()

        for i in response['items']:
            data = dict(Channel_Id = i['id'],
                    Channel_Name = i['snippet']['title'],
                    publishedat = i['snippet']['publishedAt'],
                    description = i['snippet']['description'],
                    scbscribercount = i['statistics']['subscriberCount'],
                    videocount = i['statistics']['videoCount'],
                    viewscount = i['statistics']['viewCount'],
                    Playlist_Id = i['contentDetails']['relatedPlaylists']['uploads'])

        return data

# Channel's Playlist Details

def get_playlist_details(channel_id):
  
  next_page_token=None
  All_data=[]

  while True:
    request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token)
    response = request.execute()
    
    for item in response['items']:
      data=dict(Playlist_Id=item['id'],
                Channel_Id=item['snippet']['channelId'],
                Title=item['snippet']['title'],
                Channel_Name=item['snippet']['channelTitle'],
                Published_At=item['snippet']['publishedAt'],
                Video_Count=item['contentDetails']['itemCount'])
      All_data.append(data)
    next_page_token=response.get('nextPageToken')

    if next_page_token is None:
        break

  return All_data

# Video Id's

def get_videos_ids(channel_id):
  
  video_ids=[]
  response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
  Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  next_page_token=None

  while True:
    response8=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
    for i in range(len(response8['items'])):
      video_ids.append(response8['items'][i]['snippet']['resourceId']['videoId'])
    next_page_token=response8.get('nextPageToken')
    if next_page_token is None:
      break
  return video_ids

#Function for converting the hours to Seconds  
def duration_to_seconds(duration):
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return str(total_seconds)

# Video Details

def get_video_info(video_ids):
    
    video_data=[]
    
    for Video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=Video_id
          )
        response1 = request.execute()

        for item in response1['items']:
            data=dict(channel_id=item['snippet']['channelId'],
                      Channel_Name=item['snippet']['channelTitle'],
                      video_id=item['id'],
                      video_title=item['snippet']['title'],
                      video_description=item['snippet']['description'],
                      tag=",".join(item['snippet'].get('tags',['na'])),
                      published_Date=item['snippet']['publishedAt'],
                      Duration=duration_to_seconds(item['contentDetails']['duration']),
                      viewcount=item['statistics'].get('viewCount'),
                      favoritecount=item['statistics']['favoriteCount'],
                      comment_count=item['statistics'].get('commentCount'),
                      like_count=item['statistics'].get('likeCount'),
                      Definition=item['contentDetails']['definition'],
                      Caption_status=item['contentDetails']['caption']
                      )
        video_data.append(data)

    return video_data

# Comment Details

def get_comment_details(Video_Ids):
  
  comment_data=[]
  try:
    for Video_id in Video_Ids:
      request = youtube.commentThreads().list(
              part="snippet",
              videoId=Video_id,
              maxResults=50
              )
      response = request.execute()

      for item in response['items']:
        data=dict(comment_Id=item['snippet']['topLevelComment']['id'],
                  video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                  comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                  comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                  comment_publishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt'])

        comment_data.append(data)

  except:
      pass

  return comment_data

# Data Upload

def channel_details(channel_id):
    
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_details(vi_ids)

    collect=db["channel_detail"]
    collect.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                         "video_information":vi_details,"comments_information":com_details})

    return "upload completed sucessfully"

# Channels Table creation and deatils insert

def channels_table():

    drop_query='''DROP TABLE IF EXISTS channels'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Id varchar(100) primary key, Channel_Name varchar(100),  description Text, scbscribercount bigint,
                                                            videocount int, viewscount bigint, Playlist_Id varchar(60))'''
        mycursor.execute(create_query)
        mydb.commit()
    except:
        print('Channel table is already created')


    channel_list=[]
    db=client['YouTube_Data']
    collect=db['channel_detail']
    for ch_data in collect.find({},{"_id":0,"channel_information":1}):
        channel_list.append(ch_data["channel_information"])
    df=pd.DataFrame(channel_list)

    for index,row in df.iterrows():
        insert_query='''INSERT INTO channels(Channel_Id, Channel_Name, description, scbscribercount, videocount, viewscount, Playlist_Id) 
                                            values(%s,%s,%s,%s,%s,%s,%s)''' 
        values=(row['Channel_Id'], row['Channel_Name'], row['description'], row['scbscribercount'], row['videocount'], row['viewscount'], row['Playlist_Id'])
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        except:
            print('channel value were already completed')

# Playlists Table creation and deatils insert

def playlists_table():
    
    drop_query='''DROP TABLE IF EXISTS playlists'''
    mycursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key, Channel_Id varchar(100), Title varchar(100), Channel_Name varchar(100),
                                                        Published_At timestamp, Video_Count int)'''
    mycursor.execute(create_query)
    mydb.commit()

    playlist_list=[]
    db=client['YouTube_Data']
    collect=db['channel_detail']
    for playlist_data in collect.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(playlist_data["playlist_information"])):
            playlist_list.append(playlist_data["playlist_information"][i])
    df1=pd.DataFrame(playlist_list)

    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id, Channel_Id, Title, Channel_Name, Published_At, Video_Count) values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_Id'], row['Channel_Id'], row['Title'], row['Channel_Name'], row['Published_At'], row['Video_Count'])

        mycursor.execute(insert_query,values)
        mydb.commit()

# Videos Table creation and deatils insert

def video_table():
    
    drop_query='''DROP TABLE IF EXISTS videos'''
    mycursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists videos (channel_id varchar(100), Channel_Name varchar(100), video_id varchar(60), video_title varchar(200),
                                                    video_description text, tag text, published_Date timestamp,  Duration int, viewcount int,
                                                    favoritecount int, comment_count int, like_count int, Definition varchar(80),Caption_status varchar(50))'''
    mycursor.execute(create_query)
    mydb.commit()

    videos_list=[]
    db=client['YouTube_Data']
    collect=db['channel_detail']
    for video_data in collect.find({},{"_id":0,'video_information':1}):
        for i in range(len(video_data['video_information'])):
            videos_list.append(video_data['video_information'][i])
    df2=pd.DataFrame(videos_list)

    for index,row in df2.iterrows():
        insert_query='''insert into videos(channel_id, Channel_Name, video_id, video_title, video_description, tag, published_Date, Duration, viewcount,
                                            favoritecount, comment_count, like_count, Definition, Caption_status)
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_id'], row['Channel_Name'], row['video_id'], row['video_title'], row['video_description'], row['tag'], row['published_Date'],
                row['Duration'], row['viewcount'], row['favoritecount'], row['comment_count'], row['like_count'], row['Definition'], row['Caption_status'])
        
        mycursor.execute(insert_query,values)
        mydb.commit()

# Comments Table creation and deatils insert

def comment_table():

    drop_query='''DROP TABLE IF EXISTS comments'''
    mycursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists comments(comment_Id varchar(100) primary key, video_Id varchar(50), 
                                                        comment_text Text, comment_Author varchar(150), comment_publishedAt timestamp)'''
    mycursor.execute(create_query)
    mydb.commit()

    comments_list=[]
    db=client['YouTube_Data']
    collect=db['channel_detail']
    for comment_data in collect.find({},{"_id":0,"comments_information":1}):
        for i in range(len(comment_data["comments_information"])):
            comments_list.append(comment_data["comments_information"][i])
    df3=pd.DataFrame(comments_list)

    for index,row in df3.iterrows():
        insert_query='''INSERT INTO comments(comment_Id, video_Id, comment_text, comment_Author, comment_publishedAt) values(%s,%s,%s,%s,%s)'''
        values=(row['comment_Id'],row['video_Id'],row['comment_text'],row['comment_Author'],row['comment_publishedAt'])
        mycursor.execute(insert_query,values)
        mydb.commit()

# Tables automated

def table():
    channels_table()
    playlists_table()
    video_table()
    comment_table()

    return 'Tables created successfully'    
    
def display_channels_table():
    channel_list=[] 
    db=client['YouTube_Data']
    collect=db['channel_detail']
    for ch_data in collect.find({},{"_id":0,"channel_information":1}):
        channel_list.append(ch_data["channel_information"])
    df=st.dataframe(channel_list)

    return df 

def display_playlist_table():
    playlist_list=[]
    db=client['YouTube_Data']
    collect=db['channel_detail']
    for playlist_data in collect.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(playlist_data["playlist_information"])):
            playlist_list.append(playlist_data["playlist_information"][i])
    df1=st.dataframe(playlist_list)

    return df1 

def display_video_table():
    videos_list=[]
    db=client['YouTube_Data']
    collect=db['channel_detail']
    for video_data in collect.find({},{"_id":0,'video_information':1}):
        for i in range(len(video_data['video_information'])):
            videos_list.append(video_data['video_information'][i])
    df2=st.dataframe(videos_list)

    return df2

def display_comment_table():
    comments_list=[]
    db=client['YouTube_Data']
    collect=db['channel_detail']
    for comment_data in collect.find({},{"_id":0,"comments_information":1}):
        for i in range(len(comment_data["comments_information"])):
            comments_list.append(comment_data["comments_information"][i])
    df3=st.dataframe(comments_list)

    return df3

# Streamlit part

with st.sidebar:
    st.header("Tools utilized")
    st.caption("python scripting")
    st.caption("MongoDB")
    st.caption("MySQL")
    st.caption('pandas')
    st.caption("plotly")
    st.caption("streamlit")
    

st.title(":red[YouTube] Data Harvesting and Warehousing")
channel_id=st.text_input("Enter the Channel_Id")

if st.button("Collect and Store Data"):
    channel_list=[]
    db=client['YouTube_Data']
    collect=db['channel_detail']
    for ch_data in collect.find({},{"_id":0,"channel_information":1}):
        channel_list.append(ch_data["channel_information"]["Channel_Id"])
    
    if channel_id in channel_list:
        st.success("Channel details for the given channel id is already exist")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to MySQL"):
    Tables=table()
    st.table(Tables)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNEL","PLAYLIST","VIDEOS","COMMENTS"))

if show_table=="CHANNEL":
    display_channels_table()

elif show_table=="PLAYLIST":
    display_playlist_table()

elif show_table=="VIDEOS":
    display_video_table()

elif show_table=="COMMENTS":
    display_comment_table()


# Query part

Questions = st.selectbox("Select your Questions",("1. What are the names of all the videos and their corresponding channels?",
                                                  "2. Which channels have the most number of videos, and how many videos do they have?",
                                                  "3. What are the top 10 most viewed videos and their respective channels?",
                                                  "4. How many comments were made on each video, and what are their corresponding video names?",
                                                  "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                  "6. What is the total number of likes and dislikes for each video,and what are their corresponding video names?",
                                                  "7. What if the total number of views for each channel, and what are their corresponding channel names?",
                                                  "8. What are the names of all the channels that have published videos in the your 2022?",
                                                  "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                  "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))


if Questions == "1. What are the names of all the videos and their corresponding channels?":

    Q1 = '''select video_title as videos, Channel_Name as channelname from videos'''
    mycursor.execute(Q1)
    mydb.commit()
    t1 = mycursor.fetchall()
    df = pd.DataFrame(t1,columns=['video_title','Channel_Name'])
    st.write(df)

elif Questions == "2. Which channels have the most number of videos, and how many videos do they have?":

    Q2 = '''select channel_name as Channel_Name, videocount as no_of_videos from channels order by videocount desc'''
    mycursor.execute(Q2)
    mydb.commit()
    t2 = mycursor.fetchall()
    df2 = pd.DataFrame(t2,columns=['Channel_Name','no_of_videos'])
    st.write(df2)
    fig = px.pie(df2, values='no_of_videos', names='Channel_Name')
    st.plotly_chart(fig, use_container_width=True)

elif Questions == "3. What are the top 10 most viewed videos and their respective channels?":

    Q3 = ''' select channel_name as channelname , video_title as videotitle, viewcount  from videos
             where viewcount is not null order by viewcount desc limit 10'''
    mycursor.execute(Q3)
    mydb.commit()
    t3 = mycursor.fetchall()
    df3 = pd.DataFrame(t3,columns=['channelname','videotitle','views'])
    st.write(df3)
    fig1 = px.scatter(df3, x="videotitle", y="views", color="channelname",
                 title="Top views",
                 labels={"vieotitle":"views"} 
                )
    st.plotly_chart(fig1, use_container_width=True)

elif Questions == "4. How many comments were made on each video, and what are their corresponding video names?":

    Q4 = '''select video_title as videotitle, comment_count as no_comments  from videos where comment_count is not null'''
    mycursor.execute(Q4)
    mydb.commit()
    t4 = mycursor.fetchall()
    df4 = pd.DataFrame(t4,columns=['videotitle','no_comments'])
    st.write(df4)

elif Questions == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":

    Q5 = '''select channel_name as channelname, video_title as videotitle,  like_count as likecount from videos
        where like_count is not null order by like_count desc'''
    mycursor.execute(Q5)
    mydb.commit()
    t5 = mycursor.fetchall()
    df5 = pd.DataFrame(t5,columns=['channelName','videoTitle','likeCount'])
    st.write(df5)
    Q5 = '''select channel_name as channelname, video_title as videotitle,  like_count as likecount from videos
        where like_count is not null order by like_count desc LIMIT 10'''
    mycursor.execute(Q5)
    mydb.commit()
    t5 = mycursor.fetchall()
    df51 = pd.DataFrame(t5,columns=['channelName','videoTitle','likeCount'])
    fig = px.pie(df51, values='likeCount', names='videoTitle')
    st.plotly_chart(fig, use_container_width=True)

elif Questions == "6. What is the total number of likes and dislikes for each video,and what are their corresponding video names?":

    Q6 = '''select video_title as videtitle, like_count as likecount  from videos'''
    mycursor.execute(Q6)
    mydb.commit()
    t6 = mycursor.fetchall()
    df6 = pd.DataFrame(t6,columns=['videtitle','likecount'])
    st.write(df6)

elif Questions == "7. What if the total number of views for each channel, and what are their corresponding channel names?":

    Q7 = '''select Channel_Name as channelname, viewscount as totalviews from channels'''
    mycursor.execute(Q7)
    mydb.commit()
    t7 = mycursor.fetchall()
    df7 = pd.DataFrame(t7,columns=['channelname','totalviews'])
    st.write(df7)
    fig2 = px.pie(df7, values='totalviews', names='channelname')
    st.plotly_chart(fig2, use_container_width=True)

elif Questions == "8. What are the names of all the channels that have published videos in the your 2022?":
    
    Q8 = '''select channel_name as channelname, video_title as videotitle, published_Date as videorelease from videos
        where extract(year from published_Date)=2022'''
    mycursor.execute(Q8)
    mydb.commit()
    t8 = mycursor.fetchall()
    df8 = pd.DataFrame(t8,columns=['channelName','videoTitle','publishedDate'])
    st.write(df8)
       
elif Questions == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    Q9 = '''select channel_name as channelname, avg(duration) as avgduration from videos group by channel_name'''
    mycursor.execute(Q9)
    mydb.commit()
    t9 = mycursor.fetchall()
    df9 = pd.DataFrame(t9,columns=['channelname','avgDuration'])

    t9 = []
    for index,row in df9.iterrows():
        channel_title = row['channelname']
        avgduration = row['avgDuration']
        avgduration_str = str(avgduration)
        t9.append(dict(channeltitle = channel_title, avgduration = avgduration_str))
        df9_1 = pd.DataFrame(t9)
    st.write(df9)
    fig3 = px.pie(df9, values='avgDuration', names='channelname')
    st.plotly_chart(fig3, use_container_width=True)

elif Questions == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    Q10 = '''select video_title as videotitle, channel_name as channelname, comment_count as comments from videos where comment_count 
            is not null order by comment_count desc'''
    mycursor.execute(Q10)
    mydb.commit()
    t10 = mycursor.fetchall()
    df10 = pd.DataFrame(t10,columns=['videotitle','channelname','comments'])
    st.write(df10)
    Q10 = '''select video_title as videotitle, channel_name as channelname, comment_count as comments from videos where comment_count 
            is not null order by comment_count desc LIMIT 10'''
    mycursor.execute(Q10)
    mydb.commit()
    t10 = mycursor.fetchall()
    df101 = pd.DataFrame(t10,columns=['videotitle','channelname','comments'])
    fig1 = px.scatter(df101, x="videotitle", y="comments", color="channelname",
                 title="Top Comments count",
                 labels={"vieotitle":"comments"} 
                )
    st.plotly_chart(fig1, use_container_width=True)