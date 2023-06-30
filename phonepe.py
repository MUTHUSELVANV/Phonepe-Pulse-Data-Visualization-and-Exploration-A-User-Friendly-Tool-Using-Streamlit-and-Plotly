from math import cos
import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import seaborn as sns
import numpy as np
from pymongo import MongoClient
import pymysql
import time
from datetime import datetime
import matplotlib.pyplot as plt
import plotly.express as px
import git
import os
import json
from sqlalchemy import create_engine
#import cryptography
import statistics
import geopandas as gpd
#import folium
#from streamlit_folium import st_folium

repo_url = 'https://github.com/PhonePe/pulse.git'
local_dir = 'C:/Users/admin/Desktop/Streamlit/PhonePe'

#git.Git(local_dir).clone(repo_url)

def get_data_country(amt,tu,yr,cf):
    kf=1000
    base_path = os.path.join(local_dir, f'pulse/data/aggregated/transaction/country/india/state')

    # Extract the folder name from the path
    files_and_folders = os.listdir(base_path)

    states_name = [f for f in files_and_folders if os.path.isdir(os.path.join(base_path, f))]

    if cf=='Country' and (amt == 'aggregated' or amt == 'top'):
        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{yr}')
    elif cf=='State'and (amt == 'aggregated' or amt == 'top'):
        cf1 = st.selectbox('Select the State',states_name, key=kf)
        kf+=1
        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{cf}/{cf1}/{yr}')

    if cf=='Country' and ( amt == 'map'):
        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/hover/country/india/{yr}')
    elif cf=='State' and ( amt == 'map'):
        cf1 = st.selectbox('Select the State', states_name, key=kf)
        kf+=1       
        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/hover/country/india/{cf}/{cf1}/{yr}')

    if amt == 'aggregated':
        
        file_paths = []

        sd1=[]
        sd2=[]
        sd3=[]
        sd4=[]


        if tu=='transaction':
            com=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')


                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    temp1=[]
                    temp2=[]
                    temp3=[]
                    temp4=[]

                if 'transactionData' in data['data'] and isinstance(data['data']['transactionData'], list):
                    # Iterate over the transaction data and extract relevant details
                    for j in range(len(data['data']['transactionData'])):
                        temp1.append(data['data']['transactionData'][j].get('name',0))
                        temp2.append(data['data']['transactionData'][j]['paymentInstruments'][0].get('type',''))
                        temp3.append(data['data']['transactionData'][j]['paymentInstruments'][0].get('count',0))
                        temp4.append(data['data']['transactionData'][j]['paymentInstruments'][0].get('amount',0))
                            
                sd1.append(temp1)
                sd2.append(temp2)
                sd3.append(temp3)
                sd4.append(temp4)
                
                com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount':sd4[-1]}))

                if 'transactionData' not in data['data'] and not isinstance(data['data']['transactionData'], list):
                    com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount)']))


            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2, = st.columns(2)
            with col1:
                    st.write('1st quarter')
                    st.write(com[0])
            with col2:
                    st.write('2nd quarter')
                    st.write(com[1])
            with col1:
                    st.write('3rd quarter')
                    st.write(com[2])
            with col2:
                    st.write('4th quarter')
                    st.write(com[3])

        elif tu=='user':
            agg=[]
            com=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')

                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

  
                    agg.append(data['data']['aggregated'].get('registeredUsers',0))

                    temp1=[]
                    temp2=[]
                    temp3=[]

                    if 'usersByDevice' in data['data'] and isinstance(data['data']['usersByDevice'], list):
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['usersByDevice'])):
                            temp1.append(data['data']['usersByDevice'][j].get('brand',''))
                            temp2.append(data['data']['usersByDevice'][j].get('count',0))
                            temp3.append(data['data']['usersByDevice'][j].get('percentage',0))
                                
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)

                    com.append(pd.DataFrame({'Brand': sd1[-1], 'User Count': sd2[-1], 'Percentage(%)': sd3[-1]}))

                    if 'usersByDevice' not in data['data'] and not isinstance(data['data']['usersByDevice'], list):
                        com.append(pd.DataFrame(columns=['Brand', 'User Count', 'Percentage(%)']))

            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )

            st.write('Total Registered User :',agg)
            col1,col2, = st.columns(2)
            with col1:
                    st.write('1st quarter')
                    st.write('Aggregated User: ',agg[0])
                    st.write(com[0])
            with col2:
                    st.write('2nd quarter')
                    st.write('Aggregated User: ',agg[1])
                    st.write(com[1])
            with col1:
                    st.write('3rd quarter')
                    st.write('Aggregated User: ',agg[2])
                    st.write(com[2])
            with col2:
                    st.write('4th quarter')
                    st.write('Aggregated User: ',agg[3])
                    st.write(com[3])

    if amt == 'map':
        
        file_paths = []

        sd1=[]
        sd2=[]
        sd3=[]
        sd4=[]

#        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/hover/country/india/{yr}')
        if tu=='transaction':
            com=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')


                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    temp1=[]
                    temp2=[]
                    temp3=[]
                    temp4=[]

                    if 'hoverDataList' in data['data'] and isinstance(data['data']['hoverDataList'], list):
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['hoverDataList'])):
                            temp1.append(data['data']['hoverDataList'][j].get('name',0))
                            temp2.append(data['data']['hoverDataList'][j]['metric'][0].get('type',''))
                            temp3.append(data['data']['hoverDataList'][j]['metric'][0].get('count',0))
                            temp4.append(data['data']['hoverDataList'][j]['metric'][0].get('amount',0))
                            
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)
                    sd4.append(temp4)

                    com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                    if 'hoverDataList' not in data['data'] and not isinstance(data['data']['hoverDataList'], list):
                        com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount']))

            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2, = st.columns(2)
            with col1:
                    st.write('1st quarter')
                    st.write(com[0])
            with col2:
                    st.write('2nd quarter')
                    st.write(com[1])
            with col1:
                    st.write('3rd quarter')
                    st.write(com[2])
            with col2:
                    st.write('4th quarter')
                    st.write(com[3])


#            for i in range(4):
#                if i==0:
#                    st.write(f'{i+1}st quarter')
#                    st.write(com[i])
#                elif i==1:
#                    st.write(f'{i+1}nd quarter')
#                    st.write(com[i])
#                elif i==2:
#                    st.write(f'{i+1}rd quarter')
#                    st.write(com[i])
#                elif i==3:
#                    st.write(f'{i+1}st quarter')
#                    st.write(com[i])


        elif tu=='user':
            com=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')


                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    temp1=[]
                    temp2=[]

                    if 'hoverData' in data['data'] :
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['hoverData'])):
                            temp1.append(list(data['data']['hoverData'].keys())[j])
                            temp2.append(list(data['data']['hoverData'].values())[j]['registeredUsers'])
                            
                    sd1.append(temp1)
                    sd2.append(temp2)

                    com.append(pd.DataFrame({'Name': sd1[-1], 'Registered Users': sd2[-1]}))
                
                    if 'hoverData' not in data['data']:
                        com.append(pd.DataFrame(columns=['Name','Registered Users']))

            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2, = st.columns(2)
            with col1:
                    st.write('1st quarter')
                    st.write(com[0])
            with col2:
                    st.write('2nd quarter')
                    st.write(com[1])
            with col1:
                    st.write('3rd quarter')
                    st.write(com[2])
            with col2:
                    st.write('4th quarter')
                    st.write(com[3])


    if amt == 'top' and cf == 'Country':
        
        file_paths = []

        sd1=[]
        sd2=[]
        sd3=[]
        sd4=[]
        sd5=[]
        sd6=[]
        sd7=[]
        sd8=[]
        sd9=[]
        sd10=[]
        sd11=[]
        sd12=[]
#        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{yr}')
        if tu=='transaction':
            com1=[]
            com2=[]
            com3=[]            
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')


                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    temp1=[]
                    temp2=[]
                    temp3=[]
                    temp4=[]

                    if 'states' in data['data'] and isinstance(data['data']['states'], list):
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['states'])):
                            temp1.append(data['data']['states'][j]['entityName'])
                            temp2.append(data['data']['states'][j]['metric']['type'])
                            temp3.append(data['data']['states'][j]['metric']['count'])
                            temp4.append(data['data']['states'][j]['metric']['amount'])
                            
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)
                    sd4.append(temp4)

                    com1.append(pd.DataFrame({'State Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                    if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                        com1.append(pd.DataFrame(columns=['State Name','Type', 'Count', 'Amount']))
                    
                    temp5=[]
                    temp6=[]
                    temp7=[]
                    temp8=[]

                    if 'districts' in data['data'] and isinstance(data['data']['districts'], list):                    
                        for j in range(len(data['data']['districts'])):
                            temp5.append(data['data']['districts'][j]['entityName'])
                            temp6.append(data['data']['districts'][j]['metric']['type'])
                            temp7.append(data['data']['districts'][j]['metric']['count'])
                            temp8.append(data['data']['districts'][j]['metric']['amount'])      
                    
                    sd5.append(temp5)
                    sd6.append(temp6)
                    sd7.append(temp7)
                    sd8.append(temp8)     

                    com2.append(pd.DataFrame({'District Name': sd5[-1], 'Type': sd6[-1], 'Count': sd7[-1], 'Amount': sd8[-1]}))

                    if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                        com2.append(pd.DataFrame(columns=['District Name','Type', 'Count', 'Amount']))

                    temp9=[]
                    temp10=[]
                    temp11=[]
                    temp12=[]

                    if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):                    
                        for j in range(len(data['data']['pincodes'])):
                            temp9.append(data['data']['pincodes'][j]['entityName'])
                            temp10.append(data['data']['pincodes'][j]['metric']['type'])
                            temp11.append(data['data']['pincodes'][j]['metric']['count'])
                            temp12.append(data['data']['pincodes'][j]['metric']['amount'])      

                    sd9.append(temp9)
                    sd10.append(temp10)
                    sd11.append(temp11)
                    sd12.append(temp12)                

                    com3.append(pd.DataFrame({'Pincode': sd9[-1], 'Type': sd10[-1], 'Count': sd11[-1], 'Amount': sd12[-1]}))

                    if 'pincodes' not in data['data'] and not isinstance(data['data']['pincodes'], list):
                        com3.append(pd.DataFrame(columns=['Pincode','Type', 'Count', 'Amount']))


            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2,col3 = st.columns(3)
            with col1:
                st.subheader('State Info')
                for i in range(4):
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com1[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com1[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
            with col2:
                st.subheader('District Info')
                for i in range(4):
                    
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com2[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com2[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])
            with col3:
                st.subheader('PinCode Info')   
                for i in range(4):
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com3[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com3[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com3[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com3[i])                                            


        elif tu=='user':
            com1=[]
            com2=[]
            com3=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')

                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    temp1=[]
                    temp2=[]

                    if 'states' in data['data'] and isinstance(data['data']['states'], list): 
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['states'])):
                            temp1.append(data['data']['states'][j]['name'])
                            temp2.append(data['data']['states'][j]['registeredUsers'])
                        
                    sd1.append(temp1)
                    sd2.append(temp2)

                    com1.append(pd.DataFrame({'State Name': sd1[-1], 'Registered Users': sd2[-1]}))

                    if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                        com1.append(pd.DataFrame(columns=['State Name','Registered Users']))
                    
                    
                    temp3=[]
                    temp4=[]

                    if 'districts' in data['data'] and isinstance(data['data']['districts'], list):
                        for j in range(len(data['data']['districts'])):
                            temp3.append(data['data']['districts'][j]['name'])
                            temp4.append(data['data']['districts'][j]['registeredUsers'])    
                    
                    sd3.append(temp3)
                    sd4.append(temp4)

                    com2.append(pd.DataFrame({'District Name': sd3[-1], 'Registered Users': sd4[-1]}))

                    if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                        com2.append(pd.DataFrame(columns=['District Name','Registered Users']))

                    temp5=[]
                    temp6=[]

                    if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                        for j in range(len(data['data']['pincodes'])):
                            temp5.append(data['data']['pincodes'][j]['name'])
                            temp6.append(data['data']['pincodes'][j]['registeredUsers'])     
                    
                    sd5.append(temp5)
                    sd6.append(temp6)

                    com3.append(pd.DataFrame({'Pincode': sd5[-1], 'Registered Users': sd6[-1]}))

                    if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                        com3.append(pd.DataFrame(columns=['Pincode','Registered Users']))                   

            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2,col3 = st.columns(3)
            with col1:
                st.subheader('State Info')
                for i in range(4):
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com1[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com1[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
            with col2:
                st.subheader('District Info')
                for i in range(4):
                    
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com2[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com2[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])
            with col3:
                st.subheader('PinCode Info')   
                for i in range(4):
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com3[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com3[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com3[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com3[i])            
    
    elif amt == 'top' and cf == 'State':
        file_paths = []

        sd1=[]
        sd2=[]
        sd3=[]
        sd4=[]
        sd5=[]
        sd6=[]
        sd7=[]
        sd8=[]
#        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{yr}')
        if tu=='transaction':
            com1=[]
            com2=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')


                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)


                    temp1=[]
                    temp2=[]
                    temp3=[]
                    temp4=[]
                    if 'districts' in data['data'] and isinstance(data['data']['districts'], list):
                        for j in range(len(data['data']['districts'])):
                            temp1.append(data['data']['districts'][j]['entityName'])
                            temp2.append(data['data']['districts'][j]['metric']['type'])
                            temp3.append(data['data']['districts'][j]['metric']['count'])
                            temp4.append(data['data']['districts'][j]['metric']['amount'])      
                    
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)
                    sd4.append(temp4)     

                    com1.append(pd.DataFrame({'District Name': sd1[-1], 'Type': sd2[-1],'Count': sd3[-1], 'Amount': sd4[-1]}))

                    if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                        com1.append(pd.DataFrame(columns=['District Name','Type','Count','Amount']))    

                    temp5=[]
                    temp6=[]
                    temp7=[]
                    temp8=[]

                    if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                        for j in range(len(data['data']['pincodes'])):
                            temp5.append(data['data']['pincodes'][j]['entityName'])
                            temp6.append(data['data']['pincodes'][j]['metric']['type'])
                            temp7.append(data['data']['pincodes'][j]['metric']['count'])
                            temp8.append(data['data']['pincodes'][j]['metric']['amount'])      
                    
                    sd5.append(temp5)
                    sd6.append(temp6)
                    sd7.append(temp7)
                    sd8.append(temp8)                

                    com2.append(pd.DataFrame({'PinCode': sd5[-1], 'Type': sd6[-1],'Count': sd7[-1], 'Amount': sd8[-1]}))

                    if 'pincodes' not in data['data'] and not isinstance(data['data']['pincodes'], list):
                        com2.append(pd.DataFrame(columns=['PinCode','Type','Count','Amount']))    

            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2 = st.columns(2)
            with col1:
                st.subheader('District Info')
                for i in range(4):
                    
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com1[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com1[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
            with col2:
                st.subheader('Pincode Info')   
                for i in range(4):
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com2[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com2[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])                                            


        elif tu=='user':
            com1=[]
            com2=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')

                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    
                    temp1=[]
                    temp2=[]
                    
                    if 'districts' in data['data'] and isinstance(data['data']['districts'], list):
                        for j in range(len(data['data']['districts'])):
                            temp1.append(data['data']['districts'][j]['name'])
                            temp2.append(data['data']['districts'][j]['registeredUsers'])    
                    
                    sd1.append(temp1)
                    sd2.append(temp2)

                    com1.append(pd.DataFrame({'District Name': sd1[-1], 'Registered Users': sd2[-1]}))

                    if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                        com1.append(pd.DataFrame(columns=['District Name','Registered Users']))    


                    temp3=[]
                    temp4=[]
                    
                    if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                        for j in range(len(data['data']['pincodes'])):
                            temp3.append(data['data']['pincodes'][j]['name'])
                            temp4.append(data['data']['pincodes'][j]['registeredUsers'])     
                    
                    sd3.append(temp3)
                    sd4.append(temp4)

                    com2.append(pd.DataFrame({'PinCode': sd3[-1], 'Registered Users': sd4[-1]}))

                    if 'pincodes' not in data['data'] and not isinstance(data['data']['pincodes'], list):
                        com2.append(pd.DataFrame(columns=['PinCode','Registered Users']))    

            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2 = st.columns(2)
            with col1:
                st.subheader('District Info')
                for i in range(4):      
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com1[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com1[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
            with col2:
                st.subheader('Pincode Info')   
                for i in range(4):
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com2[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com2[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])            



def mysqlimport(amt,tu,yr,cf):

    connection = pymysql.connect(host='localhost', user='root', password='Muthu#123', autocommit=True )
    host='localhost'
    user='root'
    password='Muthu#123'
    database='phonepe'

    cursor = connection.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS phonepe")
    cursor.execute("use phonepe")
    connection.commit()

    url = f"mysql+pymysql://{user}:{password}@{host}/{database}"
    # Create the SQLAlchemy engine
    engine = create_engine(url)

    if cf=='Country':
        kf=10000
        base_path = os.path.join(local_dir, f'pulse/data/aggregated/transaction/country/india/state')

        # Extract the folder name from the path
        files_and_folders = os.listdir(base_path)

        states_name = [f for f in files_and_folders if os.path.isdir(os.path.join(base_path, f))]

        if cf=='Country' and (amt == 'aggregated' or amt == 'top'):
            base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{yr}')
    #    elif cf=='State'and (amt == 'aggregated' or amt == 'top'):
    #        kf+=1
    #        cf1 = st.selectbox('Select the State',states_name, key=kf)
    #        kf+=1
    #        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{cf}/{cf1}/{yr}')
        kf+=1
        if cf=='Country' and ( amt == 'map'):
            base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/hover/country/india/{yr}')
    #    elif cf=='State' and ( amt == 'map'):
    #        kf+=1
    #       cf1 = st.selectbox('Select the State', states_name, key=kf+5)
    #      kf+=1       
    #        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/hover/country/india/{cf}/{cf1}/{yr}')

        if amt == 'aggregated':
            
            file_paths = []

            sd1=[]
            sd2=[]
            sd3=[]
            sd4=[]


            if tu=='transaction':
                com=[]
                for i in range(1,5):
                    file_path = os.path.join(base_path, f'{i}.json')


                    with open(file_path, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]
                        temp3=[]
                        temp4=[]

                    if 'transactionData' in data['data'] and isinstance(data['data']['transactionData'], list):
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['transactionData'])):
                            temp1.append(data['data']['transactionData'][j].get('name',0))
                            temp2.append(data['data']['transactionData'][j]['paymentInstruments'][0].get('type',''))
                            temp3.append(data['data']['transactionData'][j]['paymentInstruments'][0].get('count',0))
                            temp4.append(data['data']['transactionData'][j]['paymentInstruments'][0].get('amount',0))
                                
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)
                    sd4.append(temp4)
                    
                    com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount':sd4[-1]}))

                    if 'transactionData' not in data['data'] and not isinstance(data['data']['transactionData'], list):
                        com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount)']))
        
                    if cf=='Country':
                        for i in range(len(sd1)):
                            table_name = f'agg-trans-{yr}-{i+1}-India'
                            #table_name = f'quarter{i+1}'
                            com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                    elif cf=='State':
                        for i in range(len(sd1)):
                            table_name = f'agg-trans-{yr}-{i+1}-{cf1}'
                            com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False) 

            elif tu=='user':
                agg=[]
                com=[]
                for i in range(1,5):
                    file_path = os.path.join(base_path, f'{i}.json')

                    with open(file_path, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

    
                        agg.append(data['data']['aggregated'].get('registeredUsers',0))

                        temp1=[]
                        temp2=[]
                        temp3=[]

                        if 'usersByDevice' in data['data'] and isinstance(data['data']['usersByDevice'], list):
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['usersByDevice'])):
                                temp1.append(data['data']['usersByDevice'][j].get('brand',''))
                                temp2.append(data['data']['usersByDevice'][j].get('count',0))
                                temp3.append(data['data']['usersByDevice'][j].get('percentage',0))
                                    
                        sd1.append(temp1)
                        sd2.append(temp2)
                        sd3.append(temp3)

                        com.append(pd.DataFrame({'Brand': sd1[-1], 'User Count': sd2[-1], 'Percentage(%)': sd3[-1]}))

                        if 'usersByDevice' not in data['data'] and not isinstance(data['data']['usersByDevice'], list):
                            com.append(pd.DataFrame(columns=['Brand', 'User Count', 'Percentage(%)']))

                    if cf=='Country':
                        for i in range(len(sd1)):
                            table_name = f'agg-user-{yr}-{i+1}-India'
                            #table_name = f'quarter{i+1}'
                            com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                    elif cf=='State':
                        for i in range(len(sd1)):
                            table_name = f'agg-user-{yr}-{i+1}-{cf1}'
                            com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)  

        if amt == 'map':
            
            file_paths = []

            sd1=[]
            sd2=[]
            sd3=[]
            sd4=[]

    #        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/hover/country/india/{yr}')
            if tu=='transaction':
                com=[]
                for i in range(1,5):
                    file_path = os.path.join(base_path, f'{i}.json')


                    with open(file_path, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]
                        temp3=[]
                        temp4=[]

                        if 'hoverDataList' in data['data'] and isinstance(data['data']['hoverDataList'], list):
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['hoverDataList'])):
                                temp1.append(data['data']['hoverDataList'][j].get('name',0))
                                temp2.append(data['data']['hoverDataList'][j]['metric'][0].get('type',''))
                                temp3.append(data['data']['hoverDataList'][j]['metric'][0].get('count',0))
                                temp4.append(data['data']['hoverDataList'][j]['metric'][0].get('amount',0))
                                
                        sd1.append(temp1)
                        sd2.append(temp2)
                        sd3.append(temp3)
                        sd4.append(temp4)

                        com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                        if 'hoverDataList' not in data['data'] and not isinstance(data['data']['hoverDataList'], list):
                            com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount']))

                    if cf=='Country':
                        for i in range(len(sd1)):
                            table_name = f'map-trans-{yr}-{i+1}-India'
                            com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                    elif cf=='State':
                        for i in range(len(sd1)):
                            table_name = f'map-trans-{yr}-{i+1}-{cf1}'
                            com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)  


            elif tu=='user':
                com=[]
                for i in range(1,5):
                    file_path = os.path.join(base_path, f'{i}.json')


                    with open(file_path, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]

                        if 'hoverData' in data['data'] :
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['hoverData'])):
                                temp1.append(list(data['data']['hoverData'].keys())[j])
                                temp2.append(list(data['data']['hoverData'].values())[j]['registeredUsers'])
                                
                        sd1.append(temp1)
                        sd2.append(temp2)

                        com.append(pd.DataFrame({'Name': sd1[-1], 'Registered Users': sd2[-1]}))
                    
                        if 'hoverData' not in data['data']:
                            com.append(pd.DataFrame(columns=['Name','Registered Users']))

                    if cf=='Country':
                        for i in range(len(sd1)):
                            table_name = f'map-user-{yr}-{i+1}-India'
                            com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                    elif cf=='State':
                        for i in range(len(sd1)):
                            table_name = f'map-user-{yr}-{i+1}-{cf1}'
                            com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)  

        if amt == 'top' and cf == 'Country':
            
            file_paths = []

            sd1=[]
            sd2=[]
            sd3=[]
            sd4=[]
            sd5=[]
            sd6=[]
            sd7=[]
            sd8=[]
            sd9=[]
            sd10=[]
            sd11=[]
            sd12=[]
    #        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{yr}')
            if tu=='transaction':
                com1=[]
                com2=[]
                com3=[]            
                for i in range(1,5):
                    file_path = os.path.join(base_path, f'{i}.json')


                    with open(file_path, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]
                        temp3=[]
                        temp4=[]

                        if 'states' in data['data'] and isinstance(data['data']['states'], list):
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['states'])):
                                temp1.append(data['data']['states'][j]['entityName'])
                                temp2.append(data['data']['states'][j]['metric']['type'])
                                temp3.append(data['data']['states'][j]['metric']['count'])
                                temp4.append(data['data']['states'][j]['metric']['amount'])
                                
                        sd1.append(temp1)
                        sd2.append(temp2)
                        sd3.append(temp3)
                        sd4.append(temp4)

                        com1.append(pd.DataFrame({'State Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                        if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                            com1.append(pd.DataFrame(columns=['State Name','Type', 'Count', 'Amount']))
                        
                        temp5=[]
                        temp6=[]
                        temp7=[]
                        temp8=[]

                        if 'districts' in data['data'] and isinstance(data['data']['districts'], list):                    
                            for j in range(len(data['data']['districts'])):
                                temp5.append(data['data']['districts'][j]['entityName'])
                                temp6.append(data['data']['districts'][j]['metric']['type'])
                                temp7.append(data['data']['districts'][j]['metric']['count'])
                                temp8.append(data['data']['districts'][j]['metric']['amount'])      
                        
                        sd5.append(temp5)
                        sd6.append(temp6)
                        sd7.append(temp7)
                        sd8.append(temp8)     

                        com2.append(pd.DataFrame({'District Name': sd5[-1], 'Type': sd6[-1], 'Count': sd7[-1], 'Amount': sd8[-1]}))

                        if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                            com2.append(pd.DataFrame(columns=['District Name','Type', 'Count', 'Amount']))

                        temp9=[]
                        temp10=[]
                        temp11=[]
                        temp12=[]

                        if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):                    
                            for j in range(len(data['data']['pincodes'])):
                                temp9.append(data['data']['pincodes'][j]['entityName'])
                                temp10.append(data['data']['pincodes'][j]['metric']['type'])
                                temp11.append(data['data']['pincodes'][j]['metric']['count'])
                                temp12.append(data['data']['pincodes'][j]['metric']['amount'])      

                        sd9.append(temp9)
                        sd10.append(temp10)
                        sd11.append(temp11)
                        sd12.append(temp12)                

                        com3.append(pd.DataFrame({'Pincode': sd9[-1], 'Type': sd10[-1], 'Count': sd11[-1], 'Amount': sd12[-1]}))

                        if 'pincodes' not in data['data'] and not isinstance(data['data']['pincodes'], list):
                            com3.append(pd.DataFrame(columns=['Pincode','Type', 'Count', 'Amount']))

                        for i in range(len(sd1)):
                            table_name = f'top-trans-{yr}-{i+1}-statewise-India'
                            com1[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                        for i in range(len(sd5)):
                            table_name = f'top-trans-{yr}-{i+1}-districtwise-India'
                            com2[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                        for i in range(len(sd9)):
                            table_name = f'top-trans-{yr}-{i+1}-pincodewise-India'
                            com3[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)                                        


            elif tu=='user':
                com1=[]
                com2=[]
                com3=[]
                for i in range(1,5):
                    file_path = os.path.join(base_path, f'{i}.json')

                    with open(file_path, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]

                        if 'states' in data['data'] and isinstance(data['data']['states'], list): 
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['states'])):
                                temp1.append(data['data']['states'][j]['name'])
                                temp2.append(data['data']['states'][j]['registeredUsers'])
                            
                        sd1.append(temp1)
                        sd2.append(temp2)

                        com1.append(pd.DataFrame({'State Name': sd1[-1], 'Registered Users': sd2[-1]}))

                        if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                            com1.append(pd.DataFrame(columns=['State Name','Registered Users']))
                        
                        
                        temp3=[]
                        temp4=[]

                        if 'districts' in data['data'] and isinstance(data['data']['districts'], list):
                            for j in range(len(data['data']['districts'])):
                                temp3.append(data['data']['districts'][j]['name'])
                                temp4.append(data['data']['districts'][j]['registeredUsers'])    
                        
                        sd3.append(temp3)
                        sd4.append(temp4)

                        com2.append(pd.DataFrame({'District Name': sd3[-1], 'Registered Users': sd4[-1]}))

                        if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                            com2.append(pd.DataFrame(columns=['District Name','Registered Users']))

                        temp5=[]
                        temp6=[]

                        if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                            for j in range(len(data['data']['pincodes'])):
                                temp5.append(data['data']['pincodes'][j]['name'])
                                temp6.append(data['data']['pincodes'][j]['registeredUsers'])     
                        
                        sd5.append(temp5)
                        sd6.append(temp6)

                        com3.append(pd.DataFrame({'Pincode': sd5[-1], 'Registered Users': sd6[-1]}))

                        if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                            com3.append(pd.DataFrame(columns=['Pincode','Registered Users']))                   
            
                        for i in range(len(sd1)):
                            table_name = f'top-user-{yr}-{i+1}-statewise-India'
                            com1[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                        for i in range(len(sd3)):
                            table_name = f'top-user-{yr}-{i+1}-districtwise-India'
                            com2[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                        for i in range(len(sd5)):
                            table_name = f'top-user-{yr}-{i+1}-pincodewise-India'
                            com3[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)


    if cf=='State':
        kf=10000
        base_path = os.path.join(local_dir, f'pulse/data/aggregated/transaction/country/india/state')

        # Extract the folder name from the path
        files_and_folders = os.listdir(base_path)

        states_name = [f for f in files_and_folders if os.path.isdir(os.path.join(base_path, f))]

        for cf1 in states_name:
            if cf=='State'and (amt == 'aggregated' or amt == 'top'):
                kf+=1
                base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{cf}/{cf1}/{yr}')
                kf+=1

            elif cf=='State' and ( amt == 'map'):
                kf+=1 
                base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/hover/country/india/{cf}/{cf1}/{yr}')

            if amt == 'aggregated':
                
                file_paths = []

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]


                if tu=='transaction':
                    for i in range(1,5):
                        file_path = os.path.join(base_path, f'{i}.json')


                        with open(file_path, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

                            temp1=[]
                            temp2=[]
                            temp3=[]
                            temp4=[]


                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['transactionData'])):
                                temp1.append(data['data']['transactionData'][j].get('name',0))
                                temp2.append(data['data']['transactionData'][j]['paymentInstruments'][0].get('type',0))
                                temp3.append(data['data']['transactionData'][j]['paymentInstruments'][0].get('count',0))
                                temp4.append(data['data']['transactionData'][j]['paymentInstruments'][0].get('amount',0))
                                    
                            sd1.append(temp1)
                            sd2.append(temp2)
                            sd3.append(temp3)
                            sd4.append(temp4)
                        
                        com=[]
                        for i in range(len(sd1)):
                            com.append(pd.DataFrame(list(zip(sd1[i],sd2[i],sd3[i],sd4[i]))))
                        for i in range(len(sd1)):
                            com[i].columns=['Name','Type','Count','Amount']

                        if cf=='Country':
                            for i in range(len(sd1)):
                                table_name = f'agg-trans-{yr}-{i+1}-India'
                                #table_name = f'quarter{i+1}'
                                com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                        elif cf=='State':
                            for i in range(len(sd1)):
                                table_name = f'agg-trans-{yr}-{i+1}-{cf1}'
                                com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)  

                elif tu=='user':
                    agg=[]
                    com=[]
                    for i in range(1,5):
                        file_path = os.path.join(base_path, f'{i}.json')

                        with open(file_path, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

        
                            agg.append(data['data']['aggregated'].get('registeredUsers',0))

                            temp1=[]
                            temp2=[]
                            temp3=[]

                            if 'usersByDevice' in data['data'] and isinstance(data['data']['usersByDevice'], list):
                                # Iterate over the transaction data and extract relevant details
                                for j in range(len(data['data']['usersByDevice'])):
                                    temp1.append(data['data']['usersByDevice'][j].get('brand',''))
                                    temp2.append(data['data']['usersByDevice'][j].get('count',0))
                                    temp3.append(data['data']['usersByDevice'][j].get('percentage',0))
                                        
                            sd1.append(temp1)
                            sd2.append(temp2)
                            sd3.append(temp3)

                            com.append(pd.DataFrame({'Brand': sd1[-1], 'User Count': sd2[-1], 'Percentage(%)': sd3[-1]}))

                            if 'usersByDevice' not in data['data'] and not isinstance(data['data']['usersByDevice'], list):
                                com.append(pd.DataFrame(columns=['Brand', 'User Count', 'Percentage(%)']))
                        if cf=='Country':
                            for i in range(len(sd1)):
                                table_name = f'agg-user-{yr}-{i+1}-India'
                                #table_name = f'quarter{i+1}'
                                com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                        elif cf=='State':
                            for i in range(len(sd1)):
                                table_name = f'agg-user-{yr}-{i+1}-{cf1}'
                                com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)  


            if amt == 'map':
                
                file_paths = []

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]

        #        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/hover/country/india/{yr}')
                if tu=='transaction':
                    for i in range(1,5):
                        file_path = os.path.join(base_path, f'{i}.json')


                        with open(file_path, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

                            temp1=[]
                            temp2=[]
                            temp3=[]
                            temp4=[]


                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['hoverDataList'])):
                                temp1.append(data['data']['hoverDataList'][j].get('name',0))
                                temp2.append(data['data']['hoverDataList'][j]['metric'][0].get('type',0))
                                temp3.append(data['data']['hoverDataList'][j]['metric'][0].get('count',0))
                                temp4.append(data['data']['hoverDataList'][j]['metric'][0].get('amount',0))
                                    
                            sd1.append(temp1)
                            sd2.append(temp2)
                            sd3.append(temp3)
                            sd4.append(temp4)
                        
                        com=[]
                        for i in range(len(sd1)):
                            com.append(pd.DataFrame(list(zip(sd1[i],sd2[i],sd3[i],sd4[i]))))
                        for i in range(len(sd1)):
                            com[i].columns=['Name','Type','Count','Amount']

                        if cf=='Country':
                            for i in range(len(sd1)):
                                table_name = f'map-trans-{yr}-{i+1}-India'
                                com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                        elif cf=='State':
                            for i in range(len(sd1)):
                                table_name = f'map-trans-{yr}-{i+1}-{cf1}'
                                com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)  


                elif tu=='user':
                    for i in range(1,5):
                        file_path = os.path.join(base_path, f'{i}.json')

                        with open(file_path, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

                            temp1=[]
                            temp2=[]

                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['hoverData'])):
                                temp1.append(list(data['data']['hoverData'].keys())[j])
                                temp2.append(list(data['data']['hoverData'].values())[j]['registeredUsers'])
                                    
                            sd1.append(temp1)
                            sd2.append(temp2)
                        
                        com=[]
                        for i in range(len(sd1)):
                            com.append(pd.DataFrame(list(zip(sd1[i],sd2[i]))))
                        for i in range(len(sd2)):
                            com[i].columns=['Name','Registered Users']

                        if cf=='Country':
                            for i in range(len(sd1)):
                                table_name = f'map-user-{yr}-{i+1}-India'
                                com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                        elif cf=='State':
                            for i in range(len(sd1)):
                                table_name = f'map-user-{yr}-{i+1}-{cf1}'
                                com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)  

            
            if amt == 'top' and cf == 'State':
                file_paths = []

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]
                sd5=[]
                sd6=[]
                sd7=[]
                sd8=[]
        #        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{yr}')
                if tu=='transaction':
                    for i in range(1,5):
                        file_path = os.path.join(base_path, f'{i}.json')


                        with open(file_path, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)


                            temp1=[]
                            temp2=[]
                            temp3=[]
                            temp4=[]
                            
                            for j in range(len(data['data']['districts'])):
                                temp1.append(data['data']['districts'][j]['entityName'])
                                temp2.append(data['data']['districts'][j]['metric']['type'])
                                temp3.append(data['data']['districts'][j]['metric']['count'])
                                temp4.append(data['data']['districts'][j]['metric']['amount'])      
                            
                            sd1.append(temp1)
                            sd2.append(temp2)
                            sd3.append(temp3)
                            sd4.append(temp4)     

                            temp5=[]
                            temp6=[]
                            temp7=[]
                            temp8=[]
                            
                            for j in range(len(data['data']['pincodes'])):
                                temp5.append(data['data']['pincodes'][j]['entityName'])
                                temp6.append(data['data']['pincodes'][j]['metric']['type'])
                                temp7.append(data['data']['pincodes'][j]['metric']['count'])
                                temp8.append(data['data']['pincodes'][j]['metric']['amount'])      
                            
                            sd5.append(temp5)
                            sd6.append(temp6)
                            sd7.append(temp7)
                            sd8.append(temp8)                


                            com1=[]
                            for i in range(len(sd1)):
                                com1.append(pd.DataFrame(list(zip(sd1[i],sd2[i],sd3[i],sd4[i]))))
                            for i in range(len(sd1)):
                                com1[i].columns=['DistrictName','Type','Count','Amount']
                            com2=[]
                            for i in range(len(sd5)):
                                com2.append(pd.DataFrame(list(zip(sd5[i],sd6[i],sd7[i],sd8[i]))))
                            for i in range(len(sd5)):
                                com2[i].columns=['PinCode','Type','Count','Amount']

                            for i in range(len(sd1)):
                                table_name = f'top-trans-{yr}-{i+1}-{cf1}-districtwise'
                                com1[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                            for i in range(len(sd5)):

                                table_name = f'top-trans-{yr}-{i+1}-{cf1}-pincodewise'
                                com2[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)



                elif tu=='user':
                    for i in range(1,5):
                        file_path = os.path.join(base_path, f'{i}.json')

                        with open(file_path, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

                            
                            temp1=[]
                            temp2=[]
                            
                            for j in range(len(data['data']['districts'])):
                                temp1.append(data['data']['districts'][j]['name'])
                                temp2.append(data['data']['districts'][j]['registeredUsers'])    
                            
                            sd1.append(temp1)
                            sd2.append(temp2)

                            temp3=[]
                            temp4=[]
                            
                            for j in range(len(data['data']['pincodes'])):
                                temp3.append(data['data']['pincodes'][j]['name'])
                                temp4.append(data['data']['pincodes'][j]['registeredUsers'])     
                            
                            sd3.append(temp3)
                            sd4.append(temp4)
                            
                        
                            com1=[]
                            for i in range(len(sd1)):
                                com1.append(pd.DataFrame(list(zip(sd1[i],sd2[i]))))
                            for i in range(len(sd1)):
                                com1[i].columns=['DistrictName','Registered Users']
                            com2=[]
                            for i in range(len(sd3)):
                                com2.append(pd.DataFrame(list(zip(sd3[i],sd4[i]))))
                            for i in range(len(sd3)):
                                com2[i].columns=['PinCode','Registered Users']

                            for i in range(len(sd1)):
                                table_name = f'top-user-{yr}-{i+1}-{cf1}-districtwise'
                                com1[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                            for i in range(len(sd3)):

                                table_name = f'top-user-{yr}-{i+1}-{cf1}-pincodewise'
                                com2[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)

    connection.close()









def smysqlimport(amt,tu,yr,cf):

    connection = pymysql.connect(host='localhost', user='root', password='Muthu#123', autocommit=True )
    host='localhost'
    user='root'
    password='Muthu#123'
    database='phonepe'

    cursor = connection.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS phonepe")
    cursor.execute("use phonepe")
    connection.commit()

    url = f"mysql+pymysql://{user}:{password}@{host}/{database}"
    # Create the SQLAlchemy engine
    engine = create_engine(url)  

    kf=1000
    base_path = os.path.join(local_dir, f'pulse/data/aggregated/transaction/country/india/state')

    # Extract the folder name from the path
    files_and_folders = os.listdir(base_path)

    states_name = [f for f in files_and_folders if os.path.isdir(os.path.join(base_path, f))]

    if cf=='Country' and (amt == 'aggregated' or amt == 'top'):
        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{yr}')
    elif cf=='State'and (amt == 'aggregated' or amt == 'top'):
        cf1 = st.selectbox('Select the State',states_name, key=kf)
        kf+=1
        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{cf}/{cf1}/{yr}')

    if cf=='Country' and ( amt == 'map'):
        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/hover/country/india/{yr}')
    elif cf=='State' and ( amt == 'map'):
        cf1 = st.selectbox('Select the State', states_name, key=kf)
        kf+=1       
        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/hover/country/india/{cf}/{cf1}/{yr}')

    if amt == 'aggregated':
        
        file_paths = []

        sd1=[]
        sd2=[]
        sd3=[]
        sd4=[]


        if tu=='transaction':
            com=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')


                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    temp1=[]
                    temp2=[]
                    temp3=[]
                    temp4=[]

                if 'transactionData' in data['data'] and isinstance(data['data']['transactionData'], list):
                    # Iterate over the transaction data and extract relevant details
                    for j in range(len(data['data']['transactionData'])):
                        temp1.append(data['data']['transactionData'][j].get('name',0))
                        temp2.append(data['data']['transactionData'][j]['paymentInstruments'][0].get('type',''))
                        temp3.append(data['data']['transactionData'][j]['paymentInstruments'][0].get('count',0))
                        temp4.append(data['data']['transactionData'][j]['paymentInstruments'][0].get('amount',0))
                            
                sd1.append(temp1)
                sd2.append(temp2)
                sd3.append(temp3)
                sd4.append(temp4)
                
                com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount':sd4[-1]}))

                if 'transactionData' not in data['data'] and not isinstance(data['data']['transactionData'], list):
                    com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount)']))


            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2, = st.columns(2)
            with col1:
                    st.write('1st quarter')
                    st.write(com[0])
            with col2:
                    st.write('2nd quarter')
                    st.write(com[1])
            with col1:
                    st.write('3rd quarter')
                    st.write(com[2])
            with col2:
                    st.write('4th quarter')
                    st.write(com[3])

            ex = st.button('Import the above Data to Mysql')
            if ex:
                if cf=='Country':
                    for i in range(len(sd1)):
                        table_name = f'agg-trans-{yr}-{i+1}-India'
                        com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                    st.success(f'{amt}-{tu}-{yr}-{cf}--Imported')  
                elif cf=='State':
                    for i in range(len(sd1)):
                        table_name = f'agg-trans-{yr}-{i+1}-{cf1}'
                        com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)  
                    st.success(f'{amt}-{tu}-{yr}-{cf}-{cf1}--Imported') 

        elif tu=='user':
            agg=[]
            com=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')

                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

  
                    agg.append(data['data']['aggregated'].get('registeredUsers',0))

                    temp1=[]
                    temp2=[]
                    temp3=[]

                    if 'usersByDevice' in data['data'] and isinstance(data['data']['usersByDevice'], list):
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['usersByDevice'])):
                            temp1.append(data['data']['usersByDevice'][j].get('brand',''))
                            temp2.append(data['data']['usersByDevice'][j].get('count',0))
                            temp3.append(data['data']['usersByDevice'][j].get('percentage',0))
                                
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)

                    com.append(pd.DataFrame({'Brand': sd1[-1], 'User Count': sd2[-1], 'Percentage(%)': sd3[-1]}))

                    if 'usersByDevice' not in data['data'] and not isinstance(data['data']['usersByDevice'], list):
                        com.append(pd.DataFrame(columns=['Brand', 'User Count', 'Percentage(%)']))

            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )

            st.write('Total Registered User :',agg)
            col1,col2, = st.columns(2)
            with col1:
                    st.write('1st quarter')
                    st.write('Aggregated User: ',agg[0])
                    st.write(com[0])
            with col2:
                    st.write('2nd quarter')
                    st.write('Aggregated User: ',agg[1])
                    st.write(com[1])
            with col1:
                    st.write('3rd quarter')
                    st.write('Aggregated User: ',agg[2])
                    st.write(com[2])
            with col2:
                    st.write('4th quarter')
                    st.write('Aggregated User: ',agg[3])
                    st.write(com[3])

            ex = st.button('Import the above Data to Mysql')
            if ex:           
                if cf=='Country':
                    for i in range(len(sd1)):
                        table_name = f'agg-user-{yr}-{i+1}-India'
                        com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                    st.success(f'{amt}-{tu}-{yr}-{cf}--Imported')  
                elif cf=='State':
                    for i in range(len(sd1)):
                        table_name = f'agg-user-{yr}-{i+1}-{cf1}'
                        com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)  
                    st.success(f'{amt}-{tu}-{yr}-{cf}-{cf1}--Imported')  


    if amt == 'map':
        
        file_paths = []

        sd1=[]
        sd2=[]
        sd3=[]
        sd4=[]

#        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/hover/country/india/{yr}')
        if tu=='transaction':
            com=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')


                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    temp1=[]
                    temp2=[]
                    temp3=[]
                    temp4=[]

                    if 'hoverDataList' in data['data'] and isinstance(data['data']['hoverDataList'], list):
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['hoverDataList'])):
                            temp1.append(data['data']['hoverDataList'][j].get('name',0))
                            temp2.append(data['data']['hoverDataList'][j]['metric'][0].get('type',''))
                            temp3.append(data['data']['hoverDataList'][j]['metric'][0].get('count',0))
                            temp4.append(data['data']['hoverDataList'][j]['metric'][0].get('amount',0))
                            
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)
                    sd4.append(temp4)

                    com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                    if 'hoverDataList' not in data['data'] and not isinstance(data['data']['hoverDataList'], list):
                        com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount']))

            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2, = st.columns(2)
            with col1:
                    st.write('1st quarter')
                    st.write(com[0])
            with col2:
                    st.write('2nd quarter')
                    st.write(com[1])
            with col1:
                    st.write('3rd quarter')
                    st.write(com[2])
            with col2:
                    st.write('4th quarter')
                    st.write(com[3])
            ex = st.button('Import the above Data to Mysql')
            if ex:     
                if cf=='Country':
                    for i in range(len(sd1)):
                        table_name = f'map-trans-{yr}-{i+1}-India'
                        com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                    st.success(f'{amt}-{tu}-{yr}-{cf}--Imported')  
                elif cf=='State':
                    for i in range(len(sd1)):
                        table_name = f'map-trans-{yr}-{i+1}-{cf1}'
                        com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)  
                    st.success(f'{amt}-{tu}-{yr}-{cf}-{cf1}--Imported')   


        elif tu=='user':
            com=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')


                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    temp1=[]
                    temp2=[]

                    if 'hoverData' in data['data'] :
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['hoverData'])):
                            temp1.append(list(data['data']['hoverData'].keys())[j])
                            temp2.append(list(data['data']['hoverData'].values())[j]['registeredUsers'])
                            
                    sd1.append(temp1)
                    sd2.append(temp2)

                    com.append(pd.DataFrame({'Name': sd1[-1], 'Registered Users': sd2[-1]}))
                
                    if 'hoverData' not in data['data']:
                        com.append(pd.DataFrame(columns=['Name','Registered Users']))

            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2, = st.columns(2)
            with col1:
                    st.write('1st quarter')
                    st.write(com[0])
            with col2:
                    st.write('2nd quarter')
                    st.write(com[1])
            with col1:
                    st.write('3rd quarter')
                    st.write(com[2])
            with col2:
                    st.write('4th quarter')
                    st.write(com[3])

            ex = st.button('Import the above Data to Mysql')
            if ex:     
                if cf=='Country':
                    for i in range(len(sd1)):
                        table_name = f'map-user-{yr}-{i+1}-India'
                        com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                        st.success(f'{amt}-{tu}-{yr}-{cf}--Imported')  
                elif cf=='State':
                    for i in range(len(sd1)):
                        table_name = f'map-user-{yr}-{i+1}-{cf1}'
                        com[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                        st.success(f'{amt}-{tu}-{yr}-{cf}-{cf1}--Imported')     

    if amt == 'top' and cf == 'Country':
        
        file_paths = []

        sd1=[]
        sd2=[]
        sd3=[]
        sd4=[]
        sd5=[]
        sd6=[]
        sd7=[]
        sd8=[]
        sd9=[]
        sd10=[]
        sd11=[]
        sd12=[]
#        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{yr}')
        if tu=='transaction':
            com1=[]
            com2=[]
            com3=[]            
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')


                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    temp1=[]
                    temp2=[]
                    temp3=[]
                    temp4=[]

                    if 'states' in data['data'] and isinstance(data['data']['states'], list):
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['states'])):
                            temp1.append(data['data']['states'][j]['entityName'])
                            temp2.append(data['data']['states'][j]['metric']['type'])
                            temp3.append(data['data']['states'][j]['metric']['count'])
                            temp4.append(data['data']['states'][j]['metric']['amount'])
                            
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)
                    sd4.append(temp4)

                    com1.append(pd.DataFrame({'State Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                    if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                        com1.append(pd.DataFrame(columns=['State Name','Type', 'Count', 'Amount']))
                    
                    temp5=[]
                    temp6=[]
                    temp7=[]
                    temp8=[]

                    if 'districts' in data['data'] and isinstance(data['data']['districts'], list):                    
                        for j in range(len(data['data']['districts'])):
                            temp5.append(data['data']['districts'][j]['entityName'])
                            temp6.append(data['data']['districts'][j]['metric']['type'])
                            temp7.append(data['data']['districts'][j]['metric']['count'])
                            temp8.append(data['data']['districts'][j]['metric']['amount'])      
                    
                    sd5.append(temp5)
                    sd6.append(temp6)
                    sd7.append(temp7)
                    sd8.append(temp8)     

                    com2.append(pd.DataFrame({'District Name': sd5[-1], 'Type': sd6[-1], 'Count': sd7[-1], 'Amount': sd8[-1]}))

                    if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                        com2.append(pd.DataFrame(columns=['District Name','Type', 'Count', 'Amount']))

                    temp9=[]
                    temp10=[]
                    temp11=[]
                    temp12=[]

                    if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):                    
                        for j in range(len(data['data']['pincodes'])):
                            temp9.append(data['data']['pincodes'][j]['entityName'])
                            temp10.append(data['data']['pincodes'][j]['metric']['type'])
                            temp11.append(data['data']['pincodes'][j]['metric']['count'])
                            temp12.append(data['data']['pincodes'][j]['metric']['amount'])      

                    sd9.append(temp9)
                    sd10.append(temp10)
                    sd11.append(temp11)
                    sd12.append(temp12)                

                    com3.append(pd.DataFrame({'Pincode': sd9[-1], 'Type': sd10[-1], 'Count': sd11[-1], 'Amount': sd12[-1]}))

                    if 'pincodes' not in data['data'] and not isinstance(data['data']['pincodes'], list):
                        com3.append(pd.DataFrame(columns=['Pincode','Type', 'Count', 'Amount']))


            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2,col3 = st.columns(3)
            with col1:
                st.subheader('State Info')
                for i in range(4):
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com1[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com1[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
            with col2:
                st.subheader('District Info')
                for i in range(4):
                    
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com2[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com2[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])
            with col3:
                st.subheader('PinCode Info')   
                for i in range(4):
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com3[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com3[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com3[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com3[i])                                            

            ex = st.button('Import the above Data to Mysql')
            if ex:     
                for i in range(len(sd1)):
                    table_name = f'top-trans-{yr}-{i+1}-statewise-India'
                    com1[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                for i in range(len(sd1)):
                    table_name = f'top-trans-{yr}-{i+1}-districtwise-India'
                    com2[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                for i in range(len(sd1)):
                    table_name = f'top-trans-{yr}-{i+1}-pincodewise-India'
                    com3[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                st.success(f'{amt}-{tu}-{yr}-{cf}--Imported')  

        elif tu=='user':
            com1=[]
            com2=[]
            com3=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')

                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    temp1=[]
                    temp2=[]

                    if 'states' in data['data'] and isinstance(data['data']['states'], list): 
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['states'])):
                            temp1.append(data['data']['states'][j]['name'])
                            temp2.append(data['data']['states'][j]['registeredUsers'])
                        
                    sd1.append(temp1)
                    sd2.append(temp2)

                    com1.append(pd.DataFrame({'State Name': sd1[-1], 'Registered Users': sd2[-1]}))

                    if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                        com1.append(pd.DataFrame(columns=['State Name','Registered Users']))
                    
                    
                    temp3=[]
                    temp4=[]

                    if 'districts' in data['data'] and isinstance(data['data']['districts'], list):
                        for j in range(len(data['data']['districts'])):
                            temp3.append(data['data']['districts'][j]['name'])
                            temp4.append(data['data']['districts'][j]['registeredUsers'])    
                    
                    sd3.append(temp3)
                    sd4.append(temp4)

                    com2.append(pd.DataFrame({'District Name': sd3[-1], 'Registered Users': sd4[-1]}))

                    if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                        com2.append(pd.DataFrame(columns=['District Name','Registered Users']))

                    temp5=[]
                    temp6=[]

                    if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                        for j in range(len(data['data']['pincodes'])):
                            temp5.append(data['data']['pincodes'][j]['name'])
                            temp6.append(data['data']['pincodes'][j]['registeredUsers'])     
                    
                    sd5.append(temp5)
                    sd6.append(temp6)

                    com3.append(pd.DataFrame({'Pincode': sd5[-1], 'Registered Users': sd6[-1]}))

                    if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                        com3.append(pd.DataFrame(columns=['Pincode','Registered Users']))                   

            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2,col3 = st.columns(3)
            with col1:
                st.subheader('State Info')
                for i in range(4):
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com1[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com1[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
            with col2:
                st.subheader('District Info')
                for i in range(4):
                    
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com2[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com2[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])
            with col3:
                st.subheader('PinCode Info')   
                for i in range(4):
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com3[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com3[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com3[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com3[i])   

            ex = st.button('Import the above Data to Mysql')
            if ex:     
                for i in range(len(sd1)):
                    table_name = f'top-user-{yr}-{i+1}-statewise-India'
                    com1[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                for i in range(len(sd3)):
                    table_name = f'top-user-{yr}-{i+1}-districtwise-India'
                    com2[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                for i in range(len(sd5)):
                    table_name = f'top-user-{yr}-{i+1}-pincodewise-India'
                    com3[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                st.success(f'{amt}-{tu}-{yr}-{cf}--Imported')    


    elif amt == 'top' and cf == 'State':
        file_paths = []

        sd1=[]
        sd2=[]
        sd3=[]
        sd4=[]
        sd5=[]
        sd6=[]
        sd7=[]
        sd8=[]
#        base_path = os.path.join(local_dir, f'pulse/data/{amt}/{tu}/country/india/{yr}')
        if tu=='transaction':
            com1=[]
            com2=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')


                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)


                    temp1=[]
                    temp2=[]
                    temp3=[]
                    temp4=[]
                    if 'districts' in data['data'] and isinstance(data['data']['districts'], list):
                        for j in range(len(data['data']['districts'])):
                            temp1.append(data['data']['districts'][j]['entityName'])
                            temp2.append(data['data']['districts'][j]['metric']['type'])
                            temp3.append(data['data']['districts'][j]['metric']['count'])
                            temp4.append(data['data']['districts'][j]['metric']['amount'])      
                    
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)
                    sd4.append(temp4)     

                    com1.append(pd.DataFrame({'District Name': sd1[-1], 'Type': sd2[-1],'Count': sd3[-1], 'Amount': sd4[-1]}))

                    if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                        com1.append(pd.DataFrame(columns=['District Name','Type','Count','Amount']))    

                    temp5=[]
                    temp6=[]
                    temp7=[]
                    temp8=[]

                    if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                        for j in range(len(data['data']['pincodes'])):
                            temp5.append(data['data']['pincodes'][j]['entityName'])
                            temp6.append(data['data']['pincodes'][j]['metric']['type'])
                            temp7.append(data['data']['pincodes'][j]['metric']['count'])
                            temp8.append(data['data']['pincodes'][j]['metric']['amount'])      
                    
                    sd5.append(temp5)
                    sd6.append(temp6)
                    sd7.append(temp7)
                    sd8.append(temp8)                

                    com2.append(pd.DataFrame({'PinCode': sd5[-1], 'Type': sd6[-1],'Count': sd7[-1], 'Amount': sd8[-1]}))

                    if 'pincodes' not in data['data'] and not isinstance(data['data']['pincodes'], list):
                        com2.append(pd.DataFrame(columns=['PinCode','Type','Count','Amount']))    

            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2 = st.columns(2)
            with col1:
                st.subheader('District Info')
                for i in range(4):
                    
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com1[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com1[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
            with col2:
                st.subheader('Pincode Info')   
                for i in range(4):
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com2[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com2[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])                                            
            ex = st.button('Import the above Data to Mysql')
            if ex:     
                for i in range(len(sd1)):
                    table_name = f'top-trans-{yr}-{i+1}-{cf1}-districtwise'
                    com1[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                for i in range(len(sd1)):

                    table_name = f'top-trans-{yr}-{i+1}-{cf1}-pincodewise'
                    com2[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                st.success(f'{amt}-{tu}-{yr}-{cf}--Imported')                 


        elif tu=='user':
            com1=[]
            com2=[]
            for i in range(1,5):
                file_path = os.path.join(base_path, f'{i}.json')

                with open(file_path, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    
                    temp1=[]
                    temp2=[]
                    
                    if 'districts' in data['data'] and isinstance(data['data']['districts'], list):
                        for j in range(len(data['data']['districts'])):
                            temp1.append(data['data']['districts'][j]['name'])
                            temp2.append(data['data']['districts'][j]['registeredUsers'])    
                    
                    sd1.append(temp1)
                    sd2.append(temp2)

                    com1.append(pd.DataFrame({'District Name': sd1[-1], 'Registered Users': sd2[-1]}))

                    if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                        com1.append(pd.DataFrame(columns=['District Name','Registered Users']))    


                    temp3=[]
                    temp4=[]
                    
                    if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                        for j in range(len(data['data']['pincodes'])):
                            temp3.append(data['data']['pincodes'][j]['name'])
                            temp4.append(data['data']['pincodes'][j]['registeredUsers'])     
                    
                    sd3.append(temp3)
                    sd4.append(temp4)

                    com2.append(pd.DataFrame({'PinCode': sd3[-1], 'Registered Users': sd4[-1]}))

                    if 'pincodes' not in data['data'] and not isinstance(data['data']['pincodes'], list):
                        com2.append(pd.DataFrame(columns=['PinCode','Registered Users']))    

            st.subheader(f'Extracted Data of **{amt}** category of **{tu}** in the year **{yr}**' )
            col1,col2 = st.columns(2)
            with col1:
                st.subheader('District Info')
                for i in range(4):      
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com1[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com1[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com1[i])
            with col2:
                st.subheader('Pincode Info')   
                for i in range(4):
                    if i==0:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])
                    elif i==1:
                        st.write(f'{i+1}nd quarter')
                        st.write(com2[i])
                    elif i==2:
                        st.write(f'{i+1}rd quarter')
                        st.write(com2[i])
                    elif i==3:
                        st.write(f'{i+1}st quarter')
                        st.write(com2[i])      
            ex = st.button('Import the above Data to Mysql')
            if ex:    
                for i in range(len(sd1)):
                    table_name = f'top-user-{yr}-{i+1}-{cf1}-districtwise'
                    com1[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                for i in range(len(sd1)):

                    table_name = f'top-user-{yr}-{i+1}-{cf1}-pincodewise'
                    com2[i].to_sql(name=table_name.lower(), con=engine, if_exists='replace', index=False)
                st.success(f'{amt}-{tu}-{yr}-{cf}--Imported')
    connection.close()


def displaymysql():

    connection = pymysql.connect(host='localhost', user='root', password='Muthu#123', autocommit=True )
    host='localhost'
    user='root'
    password='Muthu#123'
    database='phonepe'
    kk=1

    cursor = connection.cursor()

    # Execute the query to check if the table exists
    cursor.execute("CREATE DATABASE IF NOT EXISTS phonepe;")
    cursor.execute("use phonepe;")
    table_name = 'phonepe'
    cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'phonepe';")
#    cursor.execute(query)

    # Fetch the result
    result = cursor.fetchone()

#    st.write(result[0])

    if result[0]>0:
        cursor.execute("use phonepe;")
        connection.commit()

        cursor.execute("SHOW TABLES;")
        table_names = [table[0] for table in cursor.fetchall()]

        st.write(table_names)

        pl = st.selectbox('select the table from Mysql to view the Plot',table_names,key=kk)
        kk+=1
        st.write(pl)
        cursor.execute(f"SELECT * FROM `{pl}`;")
        row = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(row, columns=column_names)
        st.dataframe(df)
    
    elif result[0]==0:
        st.error('No Data has been exported to MySql. Click Export to MySql to export to MySql')

    connection.close()


def delmysql():

    connection = pymysql.connect(host='localhost', user='root', password='Muthu#123', autocommit=True )
    host='localhost'
    user='root'
    password='Muthu#123'
    database='phonepe'
    kk=1

    cursor = connection.cursor()

    # Execute the query to check if the table exists
    cursor.execute("CREATE DATABASE IF NOT EXISTS phonepe;")
    cursor.execute("use phonepe;")
    table_name = 'phonepe'
    cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'phonepe';")
#    cursor.execute(query)

    # Fetch the result
    result = cursor.fetchone()

#    st.write(result[0])

    if result[0]>0:
        cursor.execute("use phonepe;")
        connection.commit()

        cursor.execute("SHOW TABLES;")
        table_names = [table[0] for table in cursor.fetchall()]

        st.write(table_names)

        pl = st.selectbox('select the table from Mysql to view the Plot',table_names,key=kk)
        kk+=1
#        st.write(pl)
        sk = st.button(f'Drop {pl}')
        if sk:
            cursor.execute(f"DROP TABLE `{pl}`;")
            table_names.remove(pl)
            st.success(f'Successfully Dropped {pl}')
#            pl = table_names[0] if table_names else None
#            table_name = table_names[0] if table_names else None  # Update table_name with the first table name if available
            
    elif result[0]==0:
        st.error('No Data has been exported to MySql. Click Export to MySql to export to MySql')

    connection.close()

def displayplot(am,tu):

    ypd = st.radio("Choose from the below",('yearwise Plot Display','Label Analysis','State wise Display'))   
    if ypd == 'yearwise Plot Display':
        kf=1000
        base_path = os.path.join(local_dir, f'pulse/data/{am}/{tu}/country/india')
        sbase_path = os.path.join(local_dir, f'pulse/data/{am}/{tu}/hover/country/india')
        cnt=[]
        amt=[]
        com=[]
        agg=[]
        uc=[]
        per=[]
        yr=['2018','2019','2020','2021','2022'] 
        if am == 'aggregated' and tu=='transaction':
            for i in yr:
                base_path1 = os.path.join(base_path, f'{i}')

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]


                qt = ['1','2','3','4']
                for j in qt:

                    base_path2 = os.path.join(base_path1, f'{j}.json')

                    with open(base_path2, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]
                        temp3=[]
                        temp4=[]
                        tn1 = []
                        tn2 = []

                        for k in range(len(data['data']['transactionData'])):
                            temp1.append(data['data']['transactionData'][k].get('name',0))
                            temp2.append(data['data']['transactionData'][k]['paymentInstruments'][0].get('type',''))
                            temp3.append(data['data']['transactionData'][k]['paymentInstruments'][0].get('count',0))
                            temp4.append(data['data']['transactionData'][k]['paymentInstruments'][0].get('amount',0))
                                
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)
                    sd4.append(temp4)


                    com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount':sd4[-1]}))

                    if 'transactionData' not in data['data'] and not isinstance(data['data']['transactionData'], list):
                        com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount)']))

        
            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['Count'].mean())
                cnt.append(statistics.mean(f))
            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['Amount'].mean())
                amt.append(statistics.mean(f))

            cnt = pd.DataFrame(list(zip(yr,cnt)))
            amt = pd.DataFrame(list(zip(yr,amt)))

            st.write('Comparing the Count Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Count')
            plt.xlabel('Year')
            plt.title('Comparing the Count Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            st.write('Comparing the Amount Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=amt)
            plt.ylabel('Amount')
            plt.xlabel('Year')
            plt.title('Comparing the Amount Yearwise:') 
            plt.plot(amt[0], amt[1], color='blue', marker='o')
            st.pyplot(fig)

        if am == 'aggregated' and tu=='user':
            yr =  yr=['2018','2019','2020','2021']
            for i in yr:
                base_path1 = os.path.join(base_path, f'{i}')

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]


                qt = ['1','2','3','4']
                for j in qt:

                    base_path2 = os.path.join(base_path1, f'{j}.json')

                    with open(base_path2, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)



                    agg.append(data['data']['aggregated'].get('registeredUsers',0))

                    temp1=[]
                    temp2=[]
                    temp3=[]

                    if 'usersByDevice' in data['data'] and isinstance(data['data']['usersByDevice'], list):
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['usersByDevice'])):
                            temp1.append(data['data']['usersByDevice'][j].get('brand',''))
                            temp2.append(data['data']['usersByDevice'][j].get('count',0))
                            temp3.append(data['data']['usersByDevice'][j].get('percentage',0))
                                
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)

                    com.append(pd.DataFrame({'Brand': sd1[-1], 'User Count': sd2[-1], 'Percentage(%)': sd3[-1]}))

                    if 'usersByDevice' not in data['data'] and not isinstance(data['data']['usersByDevice'], list):
                        com.append(pd.DataFrame(columns=['Brand', 'User Count', 'Percentage(%)']))


                cnt = pd.DataFrame(list(zip(yr,cnt)))
                amt = pd.DataFrame(list(zip(yr,amt)))

            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['User Count'].mean())
                uc.append(statistics.mean(f))
            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['Percentage(%)'].mean())
                per.append(statistics.mean(f))

            cnt = pd.DataFrame(list(zip(yr,uc)))
            amt = pd.DataFrame(list(zip(yr,per)))
            exx = pd.DataFrame(list(zip(yr,agg)))

            st.write(uc)

            st.write('Comparing the User Count Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Count Count')
            plt.xlabel('Year')
            plt.title('Comparing the User Count Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            st.write('Comparing the Percentage(%) Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=amt)
            plt.ylabel('Percentage(%)')
            plt.xlabel('Year')
            plt.title('Comparing the Percentage(%) Yearwise:') 
            plt.plot(amt[0], amt[1], color='blue', marker='o')
            st.pyplot(fig)

            st.write('Comparing the aggregated Registered User Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=exx)
            plt.ylabel('Aggregated Registered User')
            plt.xlabel('Year')
            plt.title('Comparing the aggregated Registered User Yearwise:') 
            plt.plot(exx[0], exx[1], color='blue', marker='o')
            st.pyplot(fig)

        if am == 'map' and tu=='transaction':
            for i in yr:
                base_path1 = os.path.join(sbase_path, f'{i}')

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]


                qt = ['1','2','3','4']
                for j in qt:

                    base_path2 = os.path.join(base_path1, f'{j}.json')

                    with open(base_path2, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]
                        temp3=[]
                        temp4=[]
                        tn1 = []
                        tn2 = []

                        if 'hoverDataList' in data['data'] and isinstance(data['data']['hoverDataList'], list):
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['hoverDataList'])):
                                temp1.append(data['data']['hoverDataList'][j].get('name',0))
                                temp2.append(data['data']['hoverDataList'][j]['metric'][0].get('type',''))
                                temp3.append(data['data']['hoverDataList'][j]['metric'][0].get('count',0))
                                temp4.append(data['data']['hoverDataList'][j]['metric'][0].get('amount',0))
                                
                        sd1.append(temp1)
                        sd2.append(temp2)
                        sd3.append(temp3)
                        sd4.append(temp4)

                        com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                        if 'hoverDataList' not in data['data'] and not isinstance(data['data']['hoverDataList'], list):
                            com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount']))
                            

        
            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['Count'].mean())
                cnt.append(statistics.mean(f))
            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['Amount'].mean())
                amt.append(statistics.mean(f))

            cnt = pd.DataFrame(list(zip(yr,cnt)))
            amt = pd.DataFrame(list(zip(yr,amt)))

            st.write('Comparing the Count Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Count')
            plt.xlabel('Year')
            plt.title('Comparing the Count Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            st.write('Comparing the Amount Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=amt)
            plt.ylabel('Amount')
            plt.xlabel('Year')
            plt.title('Comparing the Amount Yearwise:') 
            plt.plot(amt[0], amt[1], color='blue', marker='o')
            st.pyplot(fig)


        if am == 'map' and tu=='user':
            for i in yr:
                base_path1 = os.path.join(sbase_path, f'{i}')

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]


                qt = ['1','2','3','4']
                for j in qt:

                    base_path2 = os.path.join(base_path1, f'{j}.json')

                    with open(base_path2, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]

                        if 'hoverData' in data['data'] :
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['hoverData'])):
                                temp1.append(list(data['data']['hoverData'].keys())[j])
                                temp2.append(list(data['data']['hoverData'].values())[j]['registeredUsers'])
                                
                        sd1.append(temp1)
                        sd2.append(temp2)

                        com.append(pd.DataFrame({'Name': sd1[-1], 'Registered Users': sd2[-1]}))
                    

                        if 'hoverData' not in data['data'] :
                            com.append(pd.DataFrame(columns=['Name','Registered Users']))
                            

            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['Registered Users'].mean())
                cnt.append(statistics.mean(f))


            cnt = pd.DataFrame(list(zip(yr,cnt)))

            st.write('Comparing the Registered Users Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Registered Users')
            plt.xlabel('Year')
            plt.title('Comparing the Registered Users Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)

        if am == 'top' and tu=='transaction':
            com1=[]
            com2=[]
            com3=[]
            for i in yr:
                base_path1 = os.path.join(base_path, f'{i}')

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]
                sd5=[]
                sd6=[]
                sd7=[]
                sd8=[]
                sd9=[]
                sd10=[]
                sd11=[]
                sd12=[]


                qt = ['1','2','3','4']
                for j in qt:

                    base_path2 = os.path.join(base_path1, f'{j}.json')

                    with open(base_path2, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]
                        temp3=[]
                        temp4=[]

                        if 'states' in data['data'] and isinstance(data['data']['states'], list):
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['states'])):
                                temp1.append(data['data']['states'][j]['entityName'])
                                temp2.append(data['data']['states'][j]['metric']['type'])
                                temp3.append(data['data']['states'][j]['metric']['count'])
                                temp4.append(data['data']['states'][j]['metric']['amount'])
                                
                        sd1.append(temp1)
                        sd2.append(temp2)
                        sd3.append(temp3)
                        sd4.append(temp4)

                        com1.append(pd.DataFrame({'State Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                        if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                            com1.append(pd.DataFrame(columns=['State Name','Type', 'Count', 'Amount']))
                        
                        temp5=[]
                        temp6=[]
                        temp7=[]
                        temp8=[]

                        if 'districts' in data['data'] and isinstance(data['data']['districts'], list):                    
                            for j in range(len(data['data']['districts'])):
                                temp5.append(data['data']['districts'][j]['entityName'])
                                temp6.append(data['data']['districts'][j]['metric']['type'])
                                temp7.append(data['data']['districts'][j]['metric']['count'])
                                temp8.append(data['data']['districts'][j]['metric']['amount'])      
                        
                        sd5.append(temp5)
                        sd6.append(temp6)
                        sd7.append(temp7)
                        sd8.append(temp8)     

                        com2.append(pd.DataFrame({'District Name': sd5[-1], 'Type': sd6[-1], 'Count': sd7[-1], 'Amount': sd8[-1]}))

                        if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                            com2.append(pd.DataFrame(columns=['District Name','Type', 'Count', 'Amount']))

                        temp9=[]
                        temp10=[]
                        temp11=[]
                        temp12=[]

                        if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):                    
                            for j in range(len(data['data']['pincodes'])):
                                temp9.append(data['data']['pincodes'][j]['entityName'])
                                temp10.append(data['data']['pincodes'][j]['metric']['type'])
                                temp11.append(data['data']['pincodes'][j]['metric']['count'])
                                temp12.append(data['data']['pincodes'][j]['metric']['amount'])      

                        sd9.append(temp9)
                        sd10.append(temp10)
                        sd11.append(temp11)
                        sd12.append(temp12)                

                        com3.append(pd.DataFrame({'Pincode': sd9[-1], 'Type': sd10[-1], 'Count': sd11[-1], 'Amount': sd12[-1]}))

                        if 'pincodes' not in data['data'] and not isinstance(data['data']['pincodes'], list):
                            com3.append(pd.DataFrame(columns=['Pincode','Type', 'Count', 'Amount']))


            for l in range(0,len(com1),4):
                f=[]
                for m in range(4):
                    f.append(com1[l+m]['Count'].mean())
                cnt.append(statistics.mean(f))
            for l in range(0,len(com1),4):
                f=[]
                for m in range(4):
                    f.append(com1[l+m]['Amount'].mean())
                amt.append(statistics.mean(f))

            st.subheader('State Plot')
            cnt = pd.DataFrame(list(zip(yr,cnt)))
            amt = pd.DataFrame(list(zip(yr,amt)))

            st.write('Comparing the Count Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Count')
            plt.xlabel('Year')
            plt.title('Comparing the Count Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            st.write('Comparing the Amount Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=amt)
            plt.ylabel('Amount')
            plt.xlabel('Year')
            plt.title('Comparing the Amount Yearwise:') 
            plt.plot(amt[0], amt[1], color='blue', marker='o')
            st.pyplot(fig)


            cnt=[]
            amt=[]
            for l in range(0,len(com2),4):
                f=[]
                for m in range(4):
                    f.append(com2[l+m]['Count'].mean())
                cnt.append(statistics.mean(f))
            for l in range(0,len(com2),4):
                f=[]
                for m in range(4):
                    f.append(com2[l+m]['Amount'].mean())
                amt.append(statistics.mean(f))

            st.subheader('District Plot')
            cnt = pd.DataFrame(list(zip(yr,cnt)))
            amt = pd.DataFrame(list(zip(yr,amt)))

            st.write('Comparing the Count Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Count')
            plt.xlabel('Year')
            plt.title('Comparing the Count Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            st.write('Comparing the Amount Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=amt)
            plt.ylabel('Amount')
            plt.xlabel('Year')
            plt.title('Comparing the Amount Yearwise:') 
            plt.plot(amt[0], amt[1], color='blue', marker='o')
            st.pyplot(fig)

            cnt=[]
            amt=[]
            for l in range(0,len(com3),4):
                f=[]
                for m in range(4):
                    f.append(com3[l+m]['Count'].mean())
                cnt.append(statistics.mean(f))
            for l in range(0,len(com3),4):
                f=[]
                for m in range(4):
                    f.append(com3[l+m]['Amount'].mean())
                amt.append(statistics.mean(f))

            st.subheader('PinCode Plot')
            cnt = pd.DataFrame(list(zip(yr,cnt)))
            amt = pd.DataFrame(list(zip(yr,amt)))

            st.write('Comparing the Count Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Count')
            plt.xlabel('Year')
            plt.title('Comparing the Count Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            st.write('Comparing the Amount Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=amt)
            plt.ylabel('Amount')
            plt.xlabel('Year')
            plt.title('Comparing the Amount Yearwise:') 
            plt.plot(amt[0], amt[1], color='blue', marker='o')
            st.pyplot(fig)


        if am == 'top' and tu=='user':
            com1=[]
            com2=[]
            com3=[]
            for i in yr:
                base_path1 = os.path.join(base_path, f'{i}')

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]
                sd5=[]
                sd6=[]


                qt = ['1','2','3','4']
                for j in qt:

                    base_path2 = os.path.join(base_path1, f'{j}.json')

                    with open(base_path2, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]

                        if 'states' in data['data'] and isinstance(data['data']['states'], list): 
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['states'])):
                                temp1.append(data['data']['states'][j]['name'])
                                temp2.append(data['data']['states'][j]['registeredUsers'])
                            
                        sd1.append(temp1)
                        sd2.append(temp2)

                        com1.append(pd.DataFrame({'State Name': sd1[-1], 'Registered Users': sd2[-1]}))

                        if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                            com1.append(pd.DataFrame(columns=['State Name','Registered Users']))
                        
                        
                        temp3=[]
                        temp4=[]

                        if 'districts' in data['data'] and isinstance(data['data']['districts'], list):
                            for j in range(len(data['data']['districts'])):
                                temp3.append(data['data']['districts'][j]['name'])
                                temp4.append(data['data']['districts'][j]['registeredUsers'])    
                        
                        sd3.append(temp3)
                        sd4.append(temp4)

                        com2.append(pd.DataFrame({'District Name': sd3[-1], 'Registered Users': sd4[-1]}))

                        if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                            com2.append(pd.DataFrame(columns=['District Name','Registered Users']))

                        temp5=[]
                        temp6=[]

                        if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                            for j in range(len(data['data']['pincodes'])):
                                temp5.append(data['data']['pincodes'][j]['name'])
                                temp6.append(data['data']['pincodes'][j]['registeredUsers'])     
                        
                        sd5.append(temp5)
                        sd6.append(temp6)

                        com3.append(pd.DataFrame({'Pincode': sd5[-1], 'Registered Users': sd6[-1]}))

                        if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                            com3.append(pd.DataFrame(columns=['Pincode','Registered Users']))                   


            for l in range(0,len(com1),4):
                f=[]
                for m in range(4):
                    f.append(com1[l+m]['Registered Users'].mean())
                cnt.append(statistics.mean(f))


            st.subheader('State Plot')
            cnt = pd.DataFrame(list(zip(yr,cnt)))

            st.write('Comparing the Registered Users Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Registered Users')
            plt.xlabel('Year')
            plt.title('Comparing the Registered Users Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            cnt=[]
            for l in range(0,len(com2),4):
                f=[]
                for m in range(4):
                    f.append(com2[l+m]['Registered Users'].mean())
                cnt.append(statistics.mean(f))

            st.subheader('District Plot')
            cnt = pd.DataFrame(list(zip(yr,cnt)))

            st.write('Comparing the Registered Users Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Registered Users')
            plt.xlabel('Year')
            plt.title('Comparing the Registered Users Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            cnt=[]
            for l in range(0,len(com3),4):
                f=[]
                for m in range(4):
                    f.append(com3[l+m]['Registered Users'].mean())
                cnt.append(statistics.mean(f))

            st.subheader('PinCode Plot')
            cnt = pd.DataFrame(list(zip(yr,cnt)))

            st.write('Comparing the Registered Users Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Registered Users')
            plt.xlabel('Year')
            plt.title('Comparing the Registered Users Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


    if ypd == 'State wise Display':
        kf=1000
        base_path = os.path.join(local_dir, f'pulse/data/aggregated/transaction/country/india/state')
        # Extract the folder name from the path
        files_and_folders = os.listdir(base_path)

        states_name = [f for f in files_and_folders if os.path.isdir(os.path.join(base_path, f))]

        cf1 = st.selectbox('Select the State',states_name, key=kf)
        kf+=1
        base_path = os.path.join(local_dir, f'pulse/data/{am}/{tu}/country/india/state/{cf1}')
        sbase_path = os.path.join(local_dir, f'pulse/data/{am}/{tu}/hover/country/india/state/{cf1}')
        st.header(cf1)

        cnt=[]
        amt=[]
        com=[]
        agg=[]
        uc=[]
        per=[]
        yr=['2018','2019','2020','2021','2022'] 
        if am == 'aggregated' and tu=='transaction':
            for i in yr:
                base_path1 = os.path.join(base_path, f'{i}')

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]


                qt = ['1','2','3','4']
                for j in qt:

                    base_path2 = os.path.join(base_path1, f'{j}.json')

                    with open(base_path2, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]
                        temp3=[]
                        temp4=[]
                        tn1 = []
                        tn2 = []

                        for k in range(len(data['data']['transactionData'])):
                            temp1.append(data['data']['transactionData'][k].get('name',0))
                            temp2.append(data['data']['transactionData'][k]['paymentInstruments'][0].get('type',''))
                            temp3.append(data['data']['transactionData'][k]['paymentInstruments'][0].get('count',0))
                            temp4.append(data['data']['transactionData'][k]['paymentInstruments'][0].get('amount',0))
                                
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)
                    sd4.append(temp4)


                    com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount':sd4[-1]}))

                    if 'transactionData' not in data['data'] and not isinstance(data['data']['transactionData'], list):
                        com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount)']))


        
            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['Count'].mean())
                cnt.append(statistics.mean(f))
            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['Amount'].mean())
                amt.append(statistics.mean(f))

            cnt = pd.DataFrame(list(zip(yr,cnt)))
            amt = pd.DataFrame(list(zip(yr,amt)))

            st.write('Comparing the Count Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Count')
            plt.xlabel('Year')
            plt.title('Comparing the Count Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            st.write('Comparing the Amount Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=amt)
            plt.ylabel('Amount')
            plt.xlabel('Year')
            plt.title('Comparing the Amount Yearwise:') 
            plt.plot(amt[0], amt[1], color='blue', marker='o')
            st.pyplot(fig)

        if am == 'aggregated' and tu=='user':
            yr =  yr=['2018','2019','2020','2021']
            for i in yr:
                base_path1 = os.path.join(base_path, f'{i}')

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]


                qt = ['1','2','3','4']
                for j in qt:

                    base_path2 = os.path.join(base_path1, f'{j}.json')

                    with open(base_path2, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)



                    agg.append(data['data']['aggregated'].get('registeredUsers',0))

                    temp1=[]
                    temp2=[]
                    temp3=[]

                    if 'usersByDevice' in data['data'] and isinstance(data['data']['usersByDevice'], list):
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['usersByDevice'])):
                            temp1.append(data['data']['usersByDevice'][j].get('brand',''))
                            temp2.append(data['data']['usersByDevice'][j].get('count',0))
                            temp3.append(data['data']['usersByDevice'][j].get('percentage',0))
                                
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)

                    com.append(pd.DataFrame({'Brand': sd1[-1], 'User Count': sd2[-1], 'Percentage(%)': sd3[-1]}))

                    if 'usersByDevice' not in data['data'] and not isinstance(data['data']['usersByDevice'], list):
                        com.append(pd.DataFrame(columns=['Brand', 'User Count', 'Percentage(%)']))



                cnt = pd.DataFrame(list(zip(yr,cnt)))
                amt = pd.DataFrame(list(zip(yr,amt)))

            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['User Count'].mean())
                uc.append(statistics.mean(f))
            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['Percentage(%)'].mean())
                per.append(statistics.mean(f))

            cnt = pd.DataFrame(list(zip(yr,uc)))
            amt = pd.DataFrame(list(zip(yr,per)))
            exx = pd.DataFrame(list(zip(yr,agg)))

            st.write(uc)

            st.write('Comparing the User Count Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Count Count')
            plt.xlabel('Year')
            plt.title('Comparing the User Count Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            st.write('Comparing the Percentage(%) Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=amt)
            plt.ylabel('Percentage(%)')
            plt.xlabel('Year')
            plt.title('Comparing the Percentage(%) Yearwise:') 
            plt.plot(amt[0], amt[1], color='blue', marker='o')
            st.pyplot(fig)

            st.write('Comparing the aggregated Registered User Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=exx)
            plt.ylabel('Aggregated Registered User')
            plt.xlabel('Year')
            plt.title('Comparing the aggregated Registered User Yearwise:') 
            plt.plot(exx[0], exx[1], color='blue', marker='o')
            st.pyplot(fig)

        if am == 'map' and tu=='transaction':
            for i in yr:
                base_path1 = os.path.join(sbase_path, f'{i}')

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]


                qt = ['1','2','3','4']
                for j in qt:

                    base_path2 = os.path.join(base_path1, f'{j}.json')

                    with open(base_path2, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]
                        temp3=[]
                        temp4=[]
                        tn1 = []
                        tn2 = []

                        if 'hoverDataList' in data['data'] and isinstance(data['data']['hoverDataList'], list):
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['hoverDataList'])):
                                temp1.append(data['data']['hoverDataList'][j].get('name',0))
                                temp2.append(data['data']['hoverDataList'][j]['metric'][0].get('type',''))
                                temp3.append(data['data']['hoverDataList'][j]['metric'][0].get('count',0))
                                temp4.append(data['data']['hoverDataList'][j]['metric'][0].get('amount',0))
                                
                        sd1.append(temp1)
                        sd2.append(temp2)
                        sd3.append(temp3)
                        sd4.append(temp4)

                        com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                        if 'hoverDataList' not in data['data'] and not isinstance(data['data']['hoverDataList'], list):
                            com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount']))
                            

        
            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['Count'].mean())
                cnt.append(statistics.mean(f))
            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['Amount'].mean())
                amt.append(statistics.mean(f))

            cnt = pd.DataFrame(list(zip(yr,cnt)))
            amt = pd.DataFrame(list(zip(yr,amt)))

            st.write('Comparing the Count Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Count')
            plt.xlabel('Year')
            plt.title('Comparing the Count Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            st.write('Comparing the Amount Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=amt)
            plt.ylabel('Amount')
            plt.xlabel('Year')
            plt.title('Comparing the Amount Yearwise:') 
            plt.plot(amt[0], amt[1], color='blue', marker='o')
            st.pyplot(fig)


        if am == 'map' and tu=='user':
            for i in yr:
                base_path1 = os.path.join(sbase_path, f'{i}')

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]


                qt = ['1','2','3','4']
                for j in qt:

                    base_path2 = os.path.join(base_path1, f'{j}.json')

                    with open(base_path2, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)

                        temp1=[]
                        temp2=[]

                        if 'hoverData' in data['data'] :
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['hoverData'])):
                                temp1.append(list(data['data']['hoverData'].keys())[j])
                                temp2.append(list(data['data']['hoverData'].values())[j]['registeredUsers'])
                                
                        sd1.append(temp1)
                        sd2.append(temp2)

                        com.append(pd.DataFrame({'Name': sd1[-1], 'Registered Users': sd2[-1]}))
                    

                        if 'hoverData' not in data['data'] :
                            com.append(pd.DataFrame(columns=['Name','Registered Users']))
                            

            for l in range(0,len(com),4):
                f=[]
                for m in range(4):
                    f.append(com[l+m]['Registered Users'].mean())
                cnt.append(statistics.mean(f))


            cnt = pd.DataFrame(list(zip(yr,cnt)))

            st.write('Comparing the Registered Users Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Registered Users')
            plt.xlabel('Year')
            plt.title('Comparing the Registered Users Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


        if am == 'top' and tu=='transaction':
            com1=[]
            com2=[]
            com3=[]
            for i in yr:
                base_path1 = os.path.join(base_path, f'{i}')

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]
                sd5=[]
                sd6=[]
                sd7=[]
                sd8=[]
                sd9=[]
                sd10=[]
                sd11=[]
                sd12=[]


                qt = ['1','2','3','4']
                for j in qt:

                    base_path2 = os.path.join(base_path1, f'{j}.json')

                    with open(base_path2, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)
                        
                        temp5=[]
                        temp6=[]
                        temp7=[]
                        temp8=[]

                        if 'districts' in data['data'] and isinstance(data['data']['districts'], list):                    
                            for j in range(len(data['data']['districts'])):
                                temp5.append(data['data']['districts'][j]['entityName'])
                                temp6.append(data['data']['districts'][j]['metric']['type'])
                                temp7.append(data['data']['districts'][j]['metric']['count'])
                                temp8.append(data['data']['districts'][j]['metric']['amount'])      
                        
                        sd5.append(temp5)
                        sd6.append(temp6)
                        sd7.append(temp7)
                        sd8.append(temp8)     

                        com2.append(pd.DataFrame({'District Name': sd5[-1], 'Type': sd6[-1], 'Count': sd7[-1], 'Amount': sd8[-1]}))

                        if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                            com2.append(pd.DataFrame(columns=['District Name','Type', 'Count', 'Amount']))

                        temp9=[]
                        temp10=[]
                        temp11=[]
                        temp12=[]

                        if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):                    
                            for j in range(len(data['data']['pincodes'])):
                                temp9.append(data['data']['pincodes'][j]['entityName'])
                                temp10.append(data['data']['pincodes'][j]['metric']['type'])
                                temp11.append(data['data']['pincodes'][j]['metric']['count'])
                                temp12.append(data['data']['pincodes'][j]['metric']['amount'])      

                        sd9.append(temp9)
                        sd10.append(temp10)
                        sd11.append(temp11)
                        sd12.append(temp12)                

                        com3.append(pd.DataFrame({'Pincode': sd9[-1], 'Type': sd10[-1], 'Count': sd11[-1], 'Amount': sd12[-1]}))

                        if 'pincodes' not in data['data'] and not isinstance(data['data']['pincodes'], list):
                            com3.append(pd.DataFrame(columns=['Pincode','Type', 'Count', 'Amount']))


            cnt=[]
            amt=[]
            for l in range(0,len(com2),4):
                f=[]
                for m in range(4):
                    f.append(com2[l+m]['Count'].mean())
                cnt.append(statistics.mean(f))
            for l in range(0,len(com2),4):
                f=[]
                for m in range(4):
                    f.append(com2[l+m]['Amount'].mean())
                amt.append(statistics.mean(f))

            st.subheader('District Plot')
            cnt = pd.DataFrame(list(zip(yr,cnt)))
            amt = pd.DataFrame(list(zip(yr,amt)))

            st.write('Comparing the Count Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Count')
            plt.xlabel('Year')
            plt.title('Comparing the Count Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            st.write('Comparing the Amount Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=amt)
            plt.ylabel('Amount')
            plt.xlabel('Year')
            plt.title('Comparing the Amount Yearwise:') 
            plt.plot(amt[0], amt[1], color='blue', marker='o')
            st.pyplot(fig)

            cnt=[]
            amt=[]
            for l in range(0,len(com3),4):
                f=[]
                for m in range(4):
                    f.append(com3[l+m]['Count'].mean())
                cnt.append(statistics.mean(f))
            for l in range(0,len(com3),4):
                f=[]
                for m in range(4):
                    f.append(com3[l+m]['Amount'].mean())
                amt.append(statistics.mean(f))

            st.subheader('PinCode Plot')
            cnt = pd.DataFrame(list(zip(yr,cnt)))
            amt = pd.DataFrame(list(zip(yr,amt)))

            st.write('Comparing the Count Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Count')
            plt.xlabel('Year')
            plt.title('Comparing the Count Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            st.write('Comparing the Amount Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=amt)
            plt.ylabel('Amount')
            plt.xlabel('Year')
            plt.title('Comparing the Amount Yearwise:') 
            plt.plot(amt[0], amt[1], color='blue', marker='o')
            st.pyplot(fig)


        if am == 'top' and tu=='user':
            com1=[]
            com2=[]
            com3=[]
            for i in yr:
                base_path1 = os.path.join(base_path, f'{i}')

                sd1=[]
                sd2=[]
                sd3=[]
                sd4=[]
                sd5=[]
                sd6=[]


                qt = ['1','2','3','4']
                for j in qt:

                    base_path2 = os.path.join(base_path1, f'{j}.json')

                    with open(base_path2, 'r') as file:
                        data = file.read()
                        # Parse the JSON data
                        data = json.loads(data)
                        
                        
                        temp3=[]
                        temp4=[]

                        if 'districts' in data['data'] and isinstance(data['data']['districts'], list):
                            for j in range(len(data['data']['districts'])):
                                temp3.append(data['data']['districts'][j]['name'])
                                temp4.append(data['data']['districts'][j]['registeredUsers'])    
                        
                        sd3.append(temp3)
                        sd4.append(temp4)

                        com2.append(pd.DataFrame({'District Name': sd3[-1], 'Registered Users': sd4[-1]}))

                        if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                            com2.append(pd.DataFrame(columns=['District Name','Registered Users']))

                        temp5=[]
                        temp6=[]

                        if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                            for j in range(len(data['data']['pincodes'])):
                                temp5.append(data['data']['pincodes'][j]['name'])
                                temp6.append(data['data']['pincodes'][j]['registeredUsers'])     
                        
                        sd5.append(temp5)
                        sd6.append(temp6)

                        com3.append(pd.DataFrame({'Pincode': sd5[-1], 'Registered Users': sd6[-1]}))

                        if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                            com3.append(pd.DataFrame(columns=['Pincode','Registered Users']))                  


            cnt=[]
            for l in range(0,len(com2),4):
                f=[]
                for m in range(4):
                    f.append(com2[l+m]['Registered Users'].mean())
                cnt.append(statistics.mean(f))

            st.subheader('District Plot')
            cnt = pd.DataFrame(list(zip(yr,cnt)))

            st.write('Comparing the Registered Users Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Registered Users')
            plt.xlabel('Year')
            plt.title('Comparing the Registered Users Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)


            cnt=[]
            for l in range(0,len(com3),4):
                f=[]
                for m in range(4):
                    f.append(com3[l+m]['Registered Users'].mean())
                cnt.append(statistics.mean(f))

            st.subheader('PinCode Plot')
            cnt = pd.DataFrame(list(zip(yr,cnt)))

            st.write('Comparing the Registered Users Yearwise:')
            fig = plt.figure(figsize=(10, 4))
            sns.barplot(x=0,y=1,data=cnt)
            plt.ylabel('Registered Users')
            plt.xlabel('Year')
            plt.title('Comparing the Registered Users Yearwise:') 
            plt.plot(cnt[0], cnt[1], color='blue', marker='o')
            st.pyplot(fig)



    if ypd == 'Label Analysis':

        base_path = os.path.join(local_dir, f'pulse/data/{am}/{tu}/country/india')
        sbase_path = os.path.join(local_dir, f'pulse/data/{am}/{tu}/hover/country/india')
        cnt=[]
        amt=[]
        com=[]
        agg=[]
        uc=[]
        per=[]
        yr=['2018','2019','2020','2021','2022'] 




        kf=1000

        soc = ['Country','State']
        dec = st.selectbox('Select Country/State',soc, key=kf)

        kf+=1
        if dec =='Country':
            base_path = os.path.join(local_dir, f'pulse/data/{am}/{tu}/country/india')
            sbase_path = os.path.join(local_dir, f'pulse/data/{am}/{tu}/hover/country/india')         

            if am == 'aggregated' and tu=='transaction':
                for i in yr:
                    base_path1 = os.path.join(base_path, f'{i}')

                    sd1=[]
                    sd2=[]
                    sd3=[]
                    sd4=[]


                    qt = ['1','2','3','4']
                    for j in qt:

                        base_path2 = os.path.join(base_path1, f'{j}.json')

                        with open(base_path2, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

                            temp1=[]
                            temp2=[]
                            temp3=[]
                            temp4=[]
                            tn1 = []
                            tn2 = []

                            for k in range(len(data['data']['transactionData'])):
                                temp1.append(data['data']['transactionData'][k].get('name',0))
                                temp2.append(data['data']['transactionData'][k]['paymentInstruments'][0].get('type',''))
                                temp3.append(data['data']['transactionData'][k]['paymentInstruments'][0].get('count',0))
                                temp4.append(data['data']['transactionData'][k]['paymentInstruments'][0].get('amount',0))
                                    
                        sd1.append(temp1)
                        sd2.append(temp2)
                        sd3.append(temp3)
                        sd4.append(temp4)


                        com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount':sd4[-1]}))

                        if 'transactionData' not in data['data'] and not isinstance(data['data']['transactionData'], list):
                            com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount)']))


                uni_name = com[0]['Name'].unique()
                cum_count=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Name'][s]:
                                    tem1+= com[j+k]['Count'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)


                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))


                for i in range(len(cum_count)):
                    st.write(f'Comparing the count of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Count')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the count of {uni_name[i]} Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                cum_amt=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Name'][s]:
                                    tem1+= com[j+k]['Amount'][s]
                        tem2.append(tem1/4)
                    cum_amt.append(tem2)


                for i in range(len(cum_amt)):
                    amt.append(pd.DataFrame(list(zip(yr,cum_amt[i]))))

                for i in range(len(cum_amt)):
                    st.write(f'Comparing the Amount of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=amt[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Amount of {uni_name[i]} Yearwise:') 
                    plt.plot(amt[i][0], amt[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

                for i in range(len(yw)):
                    st.write(f'Comparing the Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Count')
                    plt.xlabel('Label')
                    plt.title(f'Comparing the Amount of {uni_name[i]} Yearwise:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(amt)):
                        tent=[]
                        for s in range(len(amt[j][0])):
                            if i == amt[j][0][s]:
                                tent= amt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

                for i in range(len(yw)):
                    st.write(f'Comparing the Amount for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Label')
                    plt.title(f'Comparing the Amount of {uni_name[i]} Yearwise:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)
                
            if am == 'aggregated' and tu=='user':
                yr =  yr=['2018','2019','2020','2021']
                for i in yr:
                    base_path1 = os.path.join(base_path, f'{i}')

                    sd1=[]
                    sd2=[]
                    sd3=[]
                    sd4=[]


                    qt = ['1','2','3','4']
                    for j in qt:

                        base_path2 = os.path.join(base_path1, f'{j}.json')

                        with open(base_path2, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)



                        agg.append(data['data']['aggregated'].get('registeredUsers',0))

                        temp1=[]
                        temp2=[]
                        temp3=[]

                        if 'usersByDevice' in data['data'] and isinstance(data['data']['usersByDevice'], list):
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['usersByDevice'])):
                                temp1.append(data['data']['usersByDevice'][j].get('brand',''))
                                temp2.append(data['data']['usersByDevice'][j].get('count',0))
                                temp3.append(data['data']['usersByDevice'][j].get('percentage',0))
                                    
                        sd1.append(temp1)
                        sd2.append(temp2)
                        sd3.append(temp3)

                        com.append(pd.DataFrame({'Brand': sd1[-1], 'User Count': sd2[-1], 'Percentage(%)': sd3[-1]}))

                        if 'usersByDevice' not in data['data'] and not isinstance(data['data']['usersByDevice'], list):
                            com.append(pd.DataFrame(columns=['Brand', 'User Count', 'Percentage(%)']))

    #            for i in range(len(com)):
    #                st.write(com[i])

                uni_name = com[0]['Brand'].unique()

                cum_count=[]

                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Brand'][s]:
                                    tem1+= com[j+k]['User Count'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)


                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))

                for i in range(len(cum_count)):
                    st.write(f'Comparing the count of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('User Count')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the count of {uni_name[i]} Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                cum_amt=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Brand'][s]:
                                    tem1+= com[j+k]['Percentage(%)'][s]
                        tem2.append(tem1/4)
                    cum_amt.append(tem2)


                for i in range(len(cum_amt)):
                    amt.append(pd.DataFrame(list(zip(yr,cum_amt[i]))))

                for i in range(len(cum_amt)):
                    st.write(f'Comparing the Amount of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=amt[i])
                    plt.ylabel('Percentage(%)')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Amount of {uni_name[i]} Yearwise:') 
                    plt.plot(amt[i][0], amt[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


                for i in range(len(yw)):
                    st.write(f'Comparing the Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('User Count')
                    plt.xlabel('Phone Companies')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the Count for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021']
                for i in yrs:
                    tent1=[]
                    for j in range(len(amt)):
                        tent=[]
                        for s in range(len(amt[j][0])):
                            if i == amt[j][0][s]:
                                tent= amt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

    #            st.write(yy[0][1][0][0])

                for i in range(len(yw)):
                    st.write(f'Comparing the Percentage(%) for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Percentage(%)')
                    plt.xlabel('Phone Companies')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the Percentage(%) for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

            if am == 'map' and tu=='transaction':
                for i in yr:
                    base_path1 = os.path.join(sbase_path, f'{i}')

                    sd1=[]
                    sd2=[]
                    sd3=[]
                    sd4=[]


                    qt = ['1','2','3','4']
                    for j in qt:

                        base_path2 = os.path.join(base_path1, f'{j}.json')

                        with open(base_path2, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

                            temp1=[]
                            temp2=[]
                            temp3=[]
                            temp4=[]
                            tn1 = []
                            tn2 = []

                            if 'hoverDataList' in data['data'] and isinstance(data['data']['hoverDataList'], list):
                                # Iterate over the transaction data and extract relevant details
                                for j in range(len(data['data']['hoverDataList'])):
                                    temp1.append(data['data']['hoverDataList'][j].get('name',0))
                                    temp2.append(data['data']['hoverDataList'][j]['metric'][0].get('type',''))
                                    temp3.append(data['data']['hoverDataList'][j]['metric'][0].get('count',0))
                                    temp4.append(data['data']['hoverDataList'][j]['metric'][0].get('amount',0))
                                    
                            sd1.append(temp1)
                            sd2.append(temp2)
                            sd3.append(temp3)
                            sd4.append(temp4)

                            com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                            if 'hoverDataList' not in data['data'] and not isinstance(data['data']['hoverDataList'], list):
                                com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount']))



                uni_name = com[0]['Name'].unique()

                cum_count=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Name'][s]:
                                    tem1+= com[j+k]['Count'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)



                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))
                

                for i in range(len(cum_count)):
                    st.write(f'Comparing the count of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Count')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the count of {uni_name[i]} Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                cum_amt=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Name'][s]:
                                    tem1+= com[j+k]['Amount'][s]
                        tem2.append(tem1/4)
                    cum_amt.append(tem2)


                for i in range(len(cum_amt)):
                    amt.append(pd.DataFrame(list(zip(yr,cum_amt[i]))))

                for i in range(len(cum_amt)):
                    st.write(f'Comparing the Amount of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=amt[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Amount of {uni_name[i]} Yearwise:') 
                    plt.plot(amt[i][0], amt[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

                for i in range(len(yw)):
                    st.write(f'Comparing the Label for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Count')
                    plt.xlabel('District')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the Label for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(amt)):
                        tent=[]
                        for s in range(len(amt[j][0])):
                            if i == amt[j][0][s]:
                                tent= amt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

                for i in range(len(yw)):
                    st.write(f'Comparing the Amount for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Label')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the Amount for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)




            if am == 'map' and tu=='user':
                for i in yr:
                    base_path1 = os.path.join(sbase_path, f'{i}')

                    sd1=[]
                    sd2=[]
                    sd3=[]
                    sd4=[]


                    qt = ['1','2','3','4']
                    for j in qt:

                        base_path2 = os.path.join(base_path1, f'{j}.json')

                        with open(base_path2, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

                            temp1=[]
                            temp2=[]

                            if 'hoverData' in data['data'] :
                                # Iterate over the transaction data and extract relevant details
                                for j in range(len(data['data']['hoverData'])):
                                    temp1.append(list(data['data']['hoverData'].keys())[j])
                                    temp2.append(list(data['data']['hoverData'].values())[j]['registeredUsers'])
                                    
                            sd1.append(temp1)
                            sd2.append(temp2)

                            com.append(pd.DataFrame({'Name': sd1[-1], 'Registered Users': sd2[-1]}))
                        

                            if 'hoverData' not in data['data'] :
                                com.append(pd.DataFrame(columns=['Name','Registered Users']))

                uni_name = com[0]['Name'].unique()

                cum_count=[]

                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Name'][s]:
                                    tem1+= com[j+k]['Registered Users'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)



                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))

                for i in range(len(cum_count)):
                    st.write(f'Comparing the Registered Users count of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Registered Users')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Registered Users count of {uni_name[i]} Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


                for i in range(len(yw)):
                    st.write(f'Comparing the Registered Users Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('User Count')
                    plt.xlabel('Name')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the Registered Users Count for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

            if am == 'top' and tu=='transaction':
                com1=[]
                com2=[]
                com3=[]
                for i in yr:
                    base_path1 = os.path.join(base_path, f'{i}')

                    sd1=[]
                    sd2=[]
                    sd3=[]
                    sd4=[]
                    sd5=[]
                    sd6=[]
                    sd7=[]
                    sd8=[]
                    sd9=[]
                    sd10=[]
                    sd11=[]
                    sd12=[]


                    qt = ['1','2','3','4']
                    for j in qt:

                        base_path2 = os.path.join(base_path1, f'{j}.json')

                        with open(base_path2, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

                            temp1=[]
                            temp2=[]
                            temp3=[]
                            temp4=[]

                            if 'states' in data['data'] and isinstance(data['data']['states'], list):
                                # Iterate over the transaction data and extract relevant details
                                for j in range(len(data['data']['states'])):
                                    temp1.append(data['data']['states'][j]['entityName'])
                                    temp2.append(data['data']['states'][j]['metric']['type'])
                                    temp3.append(data['data']['states'][j]['metric']['count'])
                                    temp4.append(data['data']['states'][j]['metric']['amount'])
                                    
                            sd1.append(temp1)
                            sd2.append(temp2)
                            sd3.append(temp3)
                            sd4.append(temp4)

                            com1.append(pd.DataFrame({'State Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                            if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                                com1.append(pd.DataFrame(columns=['State Name','Type', 'Count', 'Amount']))
                            
                            temp5=[]
                            temp6=[]
                            temp7=[]
                            temp8=[]

                            if 'districts' in data['data'] and isinstance(data['data']['districts'], list):                    
                                for j in range(len(data['data']['districts'])):
                                    temp5.append(data['data']['districts'][j]['entityName'])
                                    temp6.append(data['data']['districts'][j]['metric']['type'])
                                    temp7.append(data['data']['districts'][j]['metric']['count'])
                                    temp8.append(data['data']['districts'][j]['metric']['amount'])      
                            
                            sd5.append(temp5)
                            sd6.append(temp6)
                            sd7.append(temp7)
                            sd8.append(temp8)     

                            com2.append(pd.DataFrame({'District Name': sd5[-1], 'Type': sd6[-1], 'Count': sd7[-1], 'Amount': sd8[-1]}))

                            if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                                com2.append(pd.DataFrame(columns=['District Name','Type', 'Count', 'Amount']))

                            temp9=[]
                            temp10=[]
                            temp11=[]
                            temp12=[]

                            if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):                    
                                for j in range(len(data['data']['pincodes'])):
                                    temp9.append(data['data']['pincodes'][j]['entityName'])
                                    temp10.append(data['data']['pincodes'][j]['metric']['type'])
                                    temp11.append(data['data']['pincodes'][j]['metric']['count'])
                                    temp12.append(data['data']['pincodes'][j]['metric']['amount'])      

                            sd9.append(temp9)
                            sd10.append(temp10)
                            sd11.append(temp11)
                            sd12.append(temp12)                

                            com3.append(pd.DataFrame({'Pincode': sd9[-1], 'Type': sd10[-1], 'Count': sd11[-1], 'Amount': sd12[-1]}))

                            if 'pincodes' not in data['data'] and not isinstance(data['data']['pincodes'], list):
                                com3.append(pd.DataFrame(columns=['Pincode','Type', 'Count', 'Amount']))

                uni_name = com2[0]['District Name'].unique()

                cum_count=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com2),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com2[j+k]['District Name'][s]:
                                    tem1+= com2[j+k]['Count'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)

                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))


                for i in range(len(cum_count)):
                    st.write(f'Comparing the count of {uni_name[i]} District Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Count')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the count of {uni_name[i]} District Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                cum_amt=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com2),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com2[j+k]['District Name'][s]:
                                    tem1+= com2[j+k]['Amount'][s]
                        tem2.append(tem1/4)
                    cum_amt.append(tem2)



                for i in range(len(cum_amt)):
                    amt.append(pd.DataFrame(list(zip(yr,cum_amt[i]))))

                for i in range(len(cum_amt)):
                    st.write(f'Comparing the Amount of {uni_name[i]} District Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=amt[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Amount of {uni_name[i]} District Yearwise:') 
                    plt.plot(amt[i][0], amt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


                for i in range(len(yw)):
                    st.write(f'Comparing the District Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Count')
                    plt.xlabel('District Name')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the District Count for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(amt)):
                        tent=[]
                        for s in range(len(amt[j][0])):
                            if i == amt[j][0][s]:
                                tent= amt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


                for i in range(len(yw)):
                    st.write(f'Comparing the District Amount for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Amount')
                    plt.xlabel('District Name')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the District Amount for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)



                uni_name = com3[0]['Pincode'].unique()

                cum_count=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com3),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com3[j+k]['Pincode'][s]:
                                    tem1+= com3[j+k]['Count'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)


                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))
            


                for i in range(len(cum_count)):
                    st.write(f'Comparing the count of {uni_name[i]} PinCode Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Count')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the count of {uni_name[i]} PinCode Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                cum_amt=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com3),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com3[j+k]['Pincode'][s]:
                                    tem1+= com3[j+k]['Amount'][s]
                        tem2.append(tem1/4)
                    cum_amt.append(tem2)


                for i in range(len(cum_amt)):
                    amt.append(pd.DataFrame(list(zip(yr,cum_amt[i]))))

                for i in range(len(cum_amt)):
                    st.write(f'Comparing the Amount of {uni_name[i]} PinCode Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=amt[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Amount of {uni_name[i]} PinCode Yearwise:') 
                    plt.plot(amt[i][0], amt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

                for i in range(len(yw)):
                    st.write(f'Comparing the PinCode Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Count')
                    plt.xlabel('Pincode')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the PinCode Count for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(amt)):
                        tent=[]
                        for s in range(len(amt[j][0])):
                            if i == amt[j][0][s]:
                                tent= amt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


                for i in range(len(yw)):
                    st.write(f'Comparing the PinCode Amount for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Pincode')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the PinCode Amount for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

            if am == 'top' and tu=='user':
                com1=[]
                com2=[]
                com3=[]
                for i in yr:
                    base_path1 = os.path.join(base_path, f'{i}')

                    sd1=[]
                    sd2=[]
                    sd3=[]
                    sd4=[]
                    sd5=[]
                    sd6=[]


                    qt = ['1','2','3','4']
                    for j in qt:

                        base_path2 = os.path.join(base_path1, f'{j}.json')

                        with open(base_path2, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)
                            
                            
                            temp3=[]
                            temp4=[]

                            if 'districts' in data['data'] and isinstance(data['data']['districts'], list):
                                for j in range(len(data['data']['districts'])):
                                    temp3.append(data['data']['districts'][j]['name'])
                                    temp4.append(data['data']['districts'][j]['registeredUsers'])    
                            
                            sd3.append(temp3)
                            sd4.append(temp4)

                            com2.append(pd.DataFrame({'District Name': sd3[-1], 'Registered Users': sd4[-1]}))

                            if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                                com2.append(pd.DataFrame(columns=['District Name','Registered Users']))

                            temp5=[]
                            temp6=[]

                            if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                                for j in range(len(data['data']['pincodes'])):
                                    temp5.append(data['data']['pincodes'][j]['name'])
                                    temp6.append(data['data']['pincodes'][j]['registeredUsers'])     
                            
                            sd5.append(temp5)
                            sd6.append(temp6)

                            com3.append(pd.DataFrame({'Pincode': sd5[-1], 'Registered Users': sd6[-1]}))

                            if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                                com3.append(pd.DataFrame(columns=['Pincode','Registered Users']))                  
                uni_name = com2[0]['District Name'].unique()

                cum_count=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com2),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com2[j+k]['District Name'][s]:
                                    tem1+= com2[j+k]['Registered Users'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)




                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))
                



                for i in range(len(cum_count)):
                    st.write(f'Comparing the Registered Users of {uni_name[i]} District Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Registered Users')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Registered Users of {uni_name[i]} District Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


                for i in range(len(yw)):
                    st.write(f'Comparing the District Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Registered Users')
                    plt.xlabel('District Name')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the District Count for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)




                uni_name = com3[0]['Pincode'].unique()

                cum_count=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com3),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com3[j+k]['Pincode'][s]:
                                    tem1+= com3[j+k]['Registered Users'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)


                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))



                for i in range(len(cum_count)):
                    st.write(f'Comparing the Registered Users of {uni_name[i]} PinCode Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Registered Users')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Registered Users of {uni_name[i]} PinCode Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

                for i in range(len(yw)):
                    st.write(f'Comparing the PinCode Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Registered Users')
                    plt.xlabel('Pincode')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the PinCode Count for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)




        if dec =='State':
            base_path = os.path.join(local_dir, f'pulse/data/aggregated/transaction/country/india/state')
            # Extract the folder name from the path
            files_and_folders = os.listdir(base_path)

            states_name = [f for f in files_and_folders if os.path.isdir(os.path.join(base_path, f))]

            cf1 = st.selectbox('Select the State',states_name, key=kf)
            kf+=1
            base_path = os.path.join(local_dir, f'pulse/data/{am}/{tu}/country/india/state/{cf1}')
            sbase_path = os.path.join(local_dir, f'pulse/data/{am}/{tu}/hover/country/india/state/{cf1}')
            st.header(cf1,'Plots')
            cnt=[]
            amt=[]
            com=[]
            agg=[]
            uc=[]
            per=[]
            yr=['2018','2019','2020','2021','2022'] 
            if am == 'aggregated' and tu=='transaction':
                for i in yr:
                    base_path1 = os.path.join(base_path, f'{i}')

                    sd1=[]
                    sd2=[]
                    sd3=[]
                    sd4=[]


                    qt = ['1','2','3','4']
                    for j in qt:

                        base_path2 = os.path.join(base_path1, f'{j}.json')

                        with open(base_path2, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

                            temp1=[]
                            temp2=[]
                            temp3=[]
                            temp4=[]
                            tn1 = []
                            tn2 = []

                            for k in range(len(data['data']['transactionData'])):
                                temp1.append(data['data']['transactionData'][k].get('name',0))
                                temp2.append(data['data']['transactionData'][k]['paymentInstruments'][0].get('type',''))
                                temp3.append(data['data']['transactionData'][k]['paymentInstruments'][0].get('count',0))
                                temp4.append(data['data']['transactionData'][k]['paymentInstruments'][0].get('amount',0))
                                    
                        sd1.append(temp1)
                        sd2.append(temp2)
                        sd3.append(temp3)
                        sd4.append(temp4)


                        com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount':sd4[-1]}))

                        if 'transactionData' not in data['data'] and not isinstance(data['data']['transactionData'], list):
                            com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount)']))
                    
    ##           for i in range(len(com)):
    ##                st.write(com[i])

                uni_name = com[0]['Name'].unique()
    ##            st.write(uni_name)
    #            cc = st.checkbox('Count Details')
    #            if cc:
                cum_count=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Name'][s]:
                                    tem1+= com[j+k]['Count'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)


    ##            st.write(cum_count)

                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))
                
    ##            st.write(cnt)


                for i in range(len(cum_count)):
                    st.write(f'Comparing the count of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Count')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the count of {uni_name[i]} Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


    #            cc1 = st.checkbox('Amount Details')
    #            if cc1:
                cum_amt=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Name'][s]:
                                    tem1+= com[j+k]['Amount'][s]
                        tem2.append(tem1/4)
                    cum_amt.append(tem2)


    ##            st.write(cum_count)

                for i in range(len(cum_amt)):
                    amt.append(pd.DataFrame(list(zip(yr,cum_amt[i]))))

                for i in range(len(cum_amt)):
                    st.write(f'Comparing the Amount of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=amt[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Amount of {uni_name[i]} Yearwise:') 
                    plt.plot(amt[i][0], amt[i][1], color='blue', marker='o')
                    st.pyplot(fig)

    #            st.write(cnt[4])

    #            cc2 = st.checkbox('YearWise Details')
    #            if cc2:
                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

    #            st.write(yy[0][1][0][0])

                for i in range(len(yw)):
                    st.write(f'Comparing the Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Count')
                    plt.xlabel('Label')
                    plt.title(f'Comparing the Amount of {uni_name[i]} Yearwise:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(amt)):
                        tent=[]
                        for s in range(len(amt[j][0])):
                            if i == amt[j][0][s]:
                                tent= amt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

    #            st.write(yy[0][1][0][0])

                for i in range(len(yw)):
                    st.write(f'Comparing the Amount for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Label')
                    plt.title(f'Comparing the Amount of {uni_name[i]} Yearwise:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)



            if am == 'aggregated' and tu=='user':
                yr =  yr=['2018','2019','2020','2021']
                for i in yr:
                    base_path1 = os.path.join(base_path, f'{i}')

                    sd1=[]
                    sd2=[]
                    sd3=[]
                    sd4=[]


                    qt = ['1','2','3','4']
                    for j in qt:

                        base_path2 = os.path.join(base_path1, f'{j}.json')

                        with open(base_path2, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)



                        agg.append(data['data']['aggregated'].get('registeredUsers',0))

                        temp1=[]
                        temp2=[]
                        temp3=[]

                        if 'usersByDevice' in data['data'] and isinstance(data['data']['usersByDevice'], list):
                            # Iterate over the transaction data and extract relevant details
                            for j in range(len(data['data']['usersByDevice'])):
                                temp1.append(data['data']['usersByDevice'][j].get('brand',''))
                                temp2.append(data['data']['usersByDevice'][j].get('count',0))
                                temp3.append(data['data']['usersByDevice'][j].get('percentage',0))
                                    
                        sd1.append(temp1)
                        sd2.append(temp2)
                        sd3.append(temp3)

                        com.append(pd.DataFrame({'Brand': sd1[-1], 'User Count': sd2[-1], 'Percentage(%)': sd3[-1]}))

                        if 'usersByDevice' not in data['data'] and not isinstance(data['data']['usersByDevice'], list):
                            com.append(pd.DataFrame(columns=['Brand', 'User Count', 'Percentage(%)']))


    #            for i in range(len(com)):
    #                st.write(com[i])

                uni_name = com[0]['Brand'].unique()

                cum_count=[]

                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Brand'][s]:
                                    tem1+= com[j+k]['User Count'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)


                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))

                for i in range(len(cum_count)):
                    st.write(f'Comparing the count of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('User Count')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the count of {uni_name[i]} Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                cum_amt=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Brand'][s]:
                                    tem1+= com[j+k]['Percentage(%)'][s]
                        tem2.append(tem1/4)
                    cum_amt.append(tem2)


                for i in range(len(cum_amt)):
                    amt.append(pd.DataFrame(list(zip(yr,cum_amt[i]))))

                for i in range(len(cum_amt)):
                    st.write(f'Comparing the Amount of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=amt[i])
                    plt.ylabel('Percentage(%)')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Amount of {uni_name[i]} Yearwise:') 
                    plt.plot(amt[i][0], amt[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


                for i in range(len(yw)):
                    st.write(f'Comparing the Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('User Count')
                    plt.xlabel('Phone Companies')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the Count for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021']
                for i in yrs:
                    tent1=[]
                    for j in range(len(amt)):
                        tent=[]
                        for s in range(len(amt[j][0])):
                            if i == amt[j][0][s]:
                                tent= amt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

    #            st.write(yy[0][1][0][0])

                for i in range(len(yw)):
                    st.write(f'Comparing the Percentage(%) for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Percentage(%)')
                    plt.xlabel('Phone Companies')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the Percentage(%) for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)


            if am == 'map' and tu=='transaction':
                for i in yr:
                    base_path1 = os.path.join(sbase_path, f'{i}')

                    sd1=[]
                    sd2=[]
                    sd3=[]
                    sd4=[]


                    qt = ['1','2','3','4']
                    for j in qt:

                        base_path2 = os.path.join(base_path1, f'{j}.json')

                        with open(base_path2, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

                            temp1=[]
                            temp2=[]
                            temp3=[]
                            temp4=[]
                            tn1 = []
                            tn2 = []

                            if 'hoverDataList' in data['data'] and isinstance(data['data']['hoverDataList'], list):
                                # Iterate over the transaction data and extract relevant details
                                for j in range(len(data['data']['hoverDataList'])):
                                    temp1.append(data['data']['hoverDataList'][j].get('name',0))
                                    temp2.append(data['data']['hoverDataList'][j]['metric'][0].get('type',''))
                                    temp3.append(data['data']['hoverDataList'][j]['metric'][0].get('count',0))
                                    temp4.append(data['data']['hoverDataList'][j]['metric'][0].get('amount',0))
                                    
                            sd1.append(temp1)
                            sd2.append(temp2)
                            sd3.append(temp3)
                            sd4.append(temp4)

                            com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                            if 'hoverDataList' not in data['data'] and not isinstance(data['data']['hoverDataList'], list):
                                com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount']))

    ##           for i in range(len(com)):
    ##                st.write(com[i])

                uni_name = com[0]['Name'].unique()
    ##            st.write(uni_name)
    #            cc = st.checkbox('Count Details')
    #            if cc:
                cum_count=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Name'][s]:
                                    tem1+= com[j+k]['Count'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)



                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))
                

                for i in range(len(cum_count)):
                    st.write(f'Comparing the count of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Count')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the count of {uni_name[i]} Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                cum_amt=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Name'][s]:
                                    tem1+= com[j+k]['Amount'][s]
                        tem2.append(tem1/4)
                    cum_amt.append(tem2)


                for i in range(len(cum_amt)):
                    amt.append(pd.DataFrame(list(zip(yr,cum_amt[i]))))

                for i in range(len(cum_amt)):
                    st.write(f'Comparing the Amount of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=amt[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Amount of {uni_name[i]} Yearwise:') 
                    plt.plot(amt[i][0], amt[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

                for i in range(len(yw)):
                    st.write(f'Comparing the Label for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Count')
                    plt.xlabel('District')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the Label for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(amt)):
                        tent=[]
                        for s in range(len(amt[j][0])):
                            if i == amt[j][0][s]:
                                tent= amt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

                for i in range(len(yw)):
                    st.write(f'Comparing the Amount for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Label')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the Amount for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

            if am == 'map' and tu=='user':
                for i in yr:
                    base_path1 = os.path.join(sbase_path, f'{i}')

                    sd1=[]
                    sd2=[]
                    sd3=[]
                    sd4=[]


                    qt = ['1','2','3','4']
                    for j in qt:

                        base_path2 = os.path.join(base_path1, f'{j}.json')

                        with open(base_path2, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

                            temp1=[]
                            temp2=[]

                            if 'hoverData' in data['data'] :
                                # Iterate over the transaction data and extract relevant details
                                for j in range(len(data['data']['hoverData'])):
                                    temp1.append(list(data['data']['hoverData'].keys())[j])
                                    temp2.append(list(data['data']['hoverData'].values())[j]['registeredUsers'])
                                    
                            sd1.append(temp1)
                            sd2.append(temp2)

                            com.append(pd.DataFrame({'Name': sd1[-1], 'Registered Users': sd2[-1]}))
                        

                            if 'hoverData' not in data['data'] :
                                com.append(pd.DataFrame(columns=['Name','Registered Users']))

                uni_name = com[0]['Name'].unique()

                cum_count=[]

                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com[j+k]['Name'][s]:
                                    tem1+= com[j+k]['Registered Users'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)



                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))

                for i in range(len(cum_count)):
                    st.write(f'Comparing the Registered Users count of {uni_name[i]} Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Registered Users')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Registered Users count of {uni_name[i]} Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


                for i in range(len(yw)):
                    st.write(f'Comparing the Registered Users Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('User Count')
                    plt.xlabel('Name')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the Registered Users Count for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

            if am == 'top' and tu=='transaction':
                com1=[]
                com2=[]
                com3=[]
                for i in yr:
                    base_path1 = os.path.join(base_path, f'{i}')

                    sd1=[]
                    sd2=[]
                    sd3=[]
                    sd4=[]
                    sd5=[]
                    sd6=[]
                    sd7=[]
                    sd8=[]
                    sd9=[]
                    sd10=[]
                    sd11=[]
                    sd12=[]


                    qt = ['1','2','3','4']
                    for j in qt:

                        base_path2 = os.path.join(base_path1, f'{j}.json')

                        with open(base_path2, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)
                            
                            temp5=[]
                            temp6=[]
                            temp7=[]
                            temp8=[]

                            if 'districts' in data['data'] and isinstance(data['data']['districts'], list):                    
                                for j in range(len(data['data']['districts'])):
                                    temp5.append(data['data']['districts'][j]['entityName'])
                                    temp6.append(data['data']['districts'][j]['metric']['type'])
                                    temp7.append(data['data']['districts'][j]['metric']['count'])
                                    temp8.append(data['data']['districts'][j]['metric']['amount'])      
                            
                            sd5.append(temp5)
                            sd6.append(temp6)
                            sd7.append(temp7)
                            sd8.append(temp8)     

                            com2.append(pd.DataFrame({'District Name': sd5[-1], 'Type': sd6[-1], 'Count': sd7[-1], 'Amount': sd8[-1]}))

                            if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                                com2.append(pd.DataFrame(columns=['District Name','Type', 'Count', 'Amount']))

                            temp9=[]
                            temp10=[]
                            temp11=[]
                            temp12=[]

                            if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):                    
                                for j in range(len(data['data']['pincodes'])):
                                    temp9.append(data['data']['pincodes'][j]['entityName'])
                                    temp10.append(data['data']['pincodes'][j]['metric']['type'])
                                    temp11.append(data['data']['pincodes'][j]['metric']['count'])
                                    temp12.append(data['data']['pincodes'][j]['metric']['amount'])      

                            sd9.append(temp9)
                            sd10.append(temp10)
                            sd11.append(temp11)
                            sd12.append(temp12)                

                            com3.append(pd.DataFrame({'Pincode': sd9[-1], 'Type': sd10[-1], 'Count': sd11[-1], 'Amount': sd12[-1]}))

                            if 'pincodes' not in data['data'] and not isinstance(data['data']['pincodes'], list):
                                com3.append(pd.DataFrame(columns=['Pincode','Type', 'Count', 'Amount']))


                uni_name = com2[0]['District Name'].unique()

                cum_count=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com2),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com2[j+k]['District Name'][s]:
                                    tem1+= com2[j+k]['Count'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)

                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))


                for i in range(len(cum_count)):
                    st.write(f'Comparing the count of {uni_name[i]} District Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Count')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the count of {uni_name[i]} District Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                cum_amt=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com2),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com2[j+k]['District Name'][s]:
                                    tem1+= com2[j+k]['Amount'][s]
                        tem2.append(tem1/4)
                    cum_amt.append(tem2)



                for i in range(len(cum_amt)):
                    amt.append(pd.DataFrame(list(zip(yr,cum_amt[i]))))

                for i in range(len(cum_amt)):
                    st.write(f'Comparing the Amount of {uni_name[i]} District Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=amt[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Amount of {uni_name[i]} District Yearwise:') 
                    plt.plot(amt[i][0], amt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


                for i in range(len(yw)):
                    st.write(f'Comparing the District Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Count')
                    plt.xlabel('District Name')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the District Count for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(amt)):
                        tent=[]
                        for s in range(len(amt[j][0])):
                            if i == amt[j][0][s]:
                                tent= amt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


                for i in range(len(yw)):
                    st.write(f'Comparing the District Amount for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Amount')
                    plt.xlabel('District Name')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the District Amount for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)



                uni_name = com3[0]['Pincode'].unique()

                cum_count=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com3),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com3[j+k]['Pincode'][s]:
                                    tem1+= com3[j+k]['Count'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)


                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))
            


                for i in range(len(cum_count)):
                    st.write(f'Comparing the count of {uni_name[i]} PinCode Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Count')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the count of {uni_name[i]} PinCode Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                cum_amt=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com3),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com3[j+k]['Pincode'][s]:
                                    tem1+= com3[j+k]['Amount'][s]
                        tem2.append(tem1/4)
                    cum_amt.append(tem2)


                for i in range(len(cum_amt)):
                    amt.append(pd.DataFrame(list(zip(yr,cum_amt[i]))))

                for i in range(len(cum_amt)):
                    st.write(f'Comparing the Amount of {uni_name[i]} PinCode Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=amt[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Amount of {uni_name[i]} PinCode Yearwise:') 
                    plt.plot(amt[i][0], amt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

                for i in range(len(yw)):
                    st.write(f'Comparing the PinCode Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Count')
                    plt.xlabel('Pincode')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the PinCode Count for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(amt)):
                        tent=[]
                        for s in range(len(amt[j][0])):
                            if i == amt[j][0][s]:
                                tent= amt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


                for i in range(len(yw)):
                    st.write(f'Comparing the PinCode Amount for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Amount')
                    plt.xlabel('Pincode')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the PinCode Amount for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)


            if am == 'top' and tu=='user':
                com1=[]
                com2=[]
                com3=[]
                for i in yr:
                    base_path1 = os.path.join(base_path, f'{i}')

                    sd1=[]
                    sd2=[]
                    sd3=[]
                    sd4=[]
                    sd5=[]
                    sd6=[]


                    qt = ['1','2','3','4']
                    for j in qt:

                        base_path2 = os.path.join(base_path1, f'{j}.json')

                        with open(base_path2, 'r') as file:
                            data = file.read()
                            # Parse the JSON data
                            data = json.loads(data)

                            temp1=[]
                            temp2=[]

                            if 'states' in data['data'] and isinstance(data['data']['states'], list): 
                                # Iterate over the transaction data and extract relevant details
                                for j in range(len(data['data']['states'])):
                                    temp1.append(data['data']['states'][j]['name'])
                                    temp2.append(data['data']['states'][j]['registeredUsers'])
                                
                            sd1.append(temp1)
                            sd2.append(temp2)

                            com1.append(pd.DataFrame({'State Name': sd1[-1], 'Registered Users': sd2[-1]}))

                            if 'states' not in data['data'] and not isinstance(data['data']['states'], list):
                                com1.append(pd.DataFrame(columns=['State Name','Registered Users']))
                            
                            
                            temp3=[]
                            temp4=[]

                            if 'districts' in data['data'] and isinstance(data['data']['districts'], list):
                                for j in range(len(data['data']['districts'])):
                                    temp3.append(data['data']['districts'][j]['name'])
                                    temp4.append(data['data']['districts'][j]['registeredUsers'])    
                            
                            sd3.append(temp3)
                            sd4.append(temp4)

                            com2.append(pd.DataFrame({'District Name': sd3[-1], 'Registered Users': sd4[-1]}))

                            if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                                com2.append(pd.DataFrame(columns=['District Name','Registered Users']))

                            temp5=[]
                            temp6=[]

                            if 'pincodes' in data['data'] and isinstance(data['data']['pincodes'], list):
                                for j in range(len(data['data']['pincodes'])):
                                    temp5.append(data['data']['pincodes'][j]['name'])
                                    temp6.append(data['data']['pincodes'][j]['registeredUsers'])     
                            
                            sd5.append(temp5)
                            sd6.append(temp6)

                            com3.append(pd.DataFrame({'Pincode': sd5[-1], 'Registered Users': sd6[-1]}))

                            if 'districts' not in data['data'] and not isinstance(data['data']['districts'], list):
                                com3.append(pd.DataFrame(columns=['Pincode','Registered Users']))  

                uni_name = com2[0]['District Name'].unique()

                cum_count=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com2),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com2[j+k]['District Name'][s]:
                                    tem1+= com2[j+k]['Registered Users'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)




                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))
                



                for i in range(len(cum_count)):
                    st.write(f'Comparing the Registered Users of {uni_name[i]} District Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Registered Users')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Registered Users of {uni_name[i]} District Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


                for i in range(len(yw)):
                    st.write(f'Comparing the District Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Registered Users')
                    plt.xlabel('District Name')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the District Count for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)




                uni_name = com3[0]['Pincode'].unique()

                cum_count=[]
                for i in uni_name:
                    tem2 = []
                    for j in range(0,len(com3),4):
                        tem1 = 0
                        for k in range(4):
                            for s in range(len(uni_name)):
                                if i == com3[j+k]['Pincode'][s]:
                                    tem1+= com3[j+k]['Registered Users'][s]
                        tem2.append(tem1/4)
                    cum_count.append(tem2)


                for i in range(len(cum_count)):
                    cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))



                for i in range(len(cum_count)):
                    st.write(f'Comparing the Registered Users of {uni_name[i]} PinCode Yearwise:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=cnt[i])
                    plt.ylabel('Registered Users')
                    plt.xlabel('Year')
                    plt.title(f'Comparing the Registered Users of {uni_name[i]} PinCode Yearwise:') 
                    plt.plot(cnt[i][0], cnt[i][1], color='blue', marker='o')
                    st.pyplot(fig)


                yw=[]
                yrs=['2018','2019','2020','2021','2022']
                for i in yrs:
                    tent1=[]
                    for j in range(len(cnt)):
                        tent=[]
                        for s in range(len(cnt[j][0])):
                            if i == cnt[j][0][s]:
                                tent= cnt[j][1][s]
                        tent1.append(tent)
                    yw.append(tent1)


                yy=[]
                for i in range(len(yw)):
                    yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))

                for i in range(len(yw)):
                    st.write(f'Comparing the PinCode Count for the {yrs[i]}:')
                    fig = plt.figure(figsize=(10, 4))
                    sns.barplot(x=0,y=1,data=yy[i])
                    plt.ylabel('Registered Users')
                    plt.xlabel('Pincode')
                    plt.xticks(rotation=90)
                    plt.title(f'Comparing the PinCode Count for the {yrs[i]}:') 
                    plt.plot(yy[i][0], yy[i][1], color='blue', marker='o')
                    st.pyplot(fig)

def geolive(tu):
    kf=1000
    sbase_path = os.path.join(local_dir, f'pulse/data/map/{tu}/hover/country/india')
    cnt=[]
    amt=[]
    com=[]
    agg=[]
    uc=[]
    per=[]    
    yr=['2018','2019','2020','2021','2022'] 

    india_states = json.load(open(r"C:\Users\admin\Desktop\Streamlit\states_india.geojson", "r"))


    for i in range(len(india_states["features"])):
        if india_states["features"][i]["properties"]["st_nm"] == 'Daman & Diu':
            india_states["features"][i]["properties"]["st_nm"] = 'Dadra And Nagar Haveli And Daman And Diu'
        if india_states["features"][i]["properties"]["st_nm"] == 'Andaman & Nicobar Island':
            india_states["features"][i]["properties"]["st_nm"] = 'Andaman And Nicobar Islands'
    #    if india_states["features"][i]["properties"]["st_nm"] == 'Daman & Diu':
    #        india_states["features"][i]["properties"]["st_nm"] = 'Ladakh'
        if india_states["features"][i]["properties"]["st_nm"] == 'Jammu & Kashmir':
            india_states["features"][i]["properties"]["st_nm"] = 'Jammu And Kashmir'
        if india_states["features"][i]["properties"]["st_nm"] == 'Arunanchal Pradesh':
            india_states["features"][i]["properties"]["st_nm"] = 'Arunachal Pradesh'
        if india_states["features"][i]["properties"]["st_nm"] == 'NCT of Delhi':
            india_states["features"][i]["properties"]["st_nm"] = 'Delhi'

    if tu=='transaction':
        for i in yr:
            base_path1 = os.path.join(sbase_path, f'{i}')

            sd1=[]
            sd2=[]
            sd3=[]
            sd4=[]


            qt = ['1','2','3','4']
            for j in qt:

                base_path2 = os.path.join(base_path1, f'{j}.json')

                with open(base_path2, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    temp1=[]
                    temp2=[]
                    temp3=[]
                    temp4=[]
                    tn1 = []
                    tn2 = []

                    if 'hoverDataList' in data['data'] and isinstance(data['data']['hoverDataList'], list):
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['hoverDataList'])):
                            temp1.append(data['data']['hoverDataList'][j].get('name',0))
                            temp2.append(data['data']['hoverDataList'][j]['metric'][0].get('type',''))
                            temp3.append(data['data']['hoverDataList'][j]['metric'][0].get('count',0))
                            temp4.append(data['data']['hoverDataList'][j]['metric'][0].get('amount',0))
                            
                    sd1.append(temp1)
                    sd2.append(temp2)
                    sd3.append(temp3)
                    sd4.append(temp4)

                    com.append(pd.DataFrame({'Name': sd1[-1], 'Type': sd2[-1], 'Count': sd3[-1], 'Amount': sd4[-1]}))

                    if 'hoverDataList' not in data['data'] and not isinstance(data['data']['hoverDataList'], list):
                        com.append(pd.DataFrame(columns=['Name','Type', 'Count', 'Amount']))


        uni_name = com[0]['Name'].unique()

        cum_count=[]
        for i in uni_name:
            tem2 = []
            for j in range(0,len(com),4):
                tem1 = 0
                for k in range(4):
                    for s in range(len(uni_name)):
                        if i == com[j+k]['Name'][s]:
                            tem1+= com[j+k]['Count'][s]
                tem2.append(tem1/4)
            cum_count.append(tem2)



        for i in range(len(cum_count)):
            cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))

        cum_amt=[]
        for i in uni_name:
            tem2 = []
            for j in range(0,len(com),4):
                tem1 = 0
                for k in range(4):
                    for s in range(len(uni_name)):
                        if i == com[j+k]['Name'][s]:
                            tem1+= com[j+k]['Amount'][s]
                tem2.append(tem1/4)
            cum_amt.append(tem2)


        for i in range(len(cum_amt)):
            amt.append(pd.DataFrame(list(zip(yr,cum_amt[i]))))


        yw=[]
        yrs=['2018','2019','2020','2021','2022']
        for i in yrs:
            tent1=[]
            for j in range(len(cnt)):
                tent=[]
                for s in range(len(cnt[j][0])):
                    if i == cnt[j][0][s]:
                        tent= cnt[j][1][s]
                tent1.append(tent)
            yw.append(tent1)



        yy=[]
        for i in range(len(yw)):
            yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


        yw=[]
        yrs=['2018','2019','2020','2021','2022']
        for i in yrs:
            tent1=[]
            for j in range(len(amt)):
                tent=[]
                for s in range(len(amt[j][0])):
                    if i == amt[j][0][s]:
                        tent= amt[j][1][s]
                tent1.append(tent)
            yw.append(tent1)


        yx=[]
        for i in range(len(yw)):
            yx.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


        state_id_map = {}
        for feature in india_states["features"]:
            feature["id"] = feature["properties"]["state_code"]
            state_id_map[feature["properties"]["st_nm"]] = feature["id"]
    
        kky=100
        opt=['2018','2019','2020','2021','2022']
        year= st.selectbox('Select the year', opt, key=kky)
        kky+=1
        i = opt.index(year)

        yy[i][0] = yy[i][0].apply(lambda x:x.replace("&", "and") if('&' in x) else x)

        yy[i][0] = yy[i][0].apply(lambda x: x.title())
        yy[i] = yy[i][yy[i][0]!='Ladakh']
#        st.write(yy) 
        yy[i]["id1"] = yy[i][0].apply(lambda x: state_id_map[x])

        yy[i] = yy[i].rename(columns={0: 'Name',1: 'Count'})

        df = pd.concat([yy[i], yx[i]], axis=1)
        df = df.iloc[:,[0,1,2,4]]
        df = df.rename(columns={1: 'Amount'})
#        st.write(df)
 
        st.header('Transaction - ')
        st.header(f"National Count & Amount Data - India - {year}")
        fig = px.choropleth(
            df,
            locations="id1",
            geojson=india_states,
            color='Count',
            hover_name='Name',
            hover_data=['Count', 'Amount'],
            title=f"National Count Data - India - {year}",
            color_continuous_scale="Blues", # "Viridis", "YlOrRd", "Blues", 
        )
        fig.update_geos(fitbounds="locations", visible=False)

        # Set the layout properties
        fig.update_layout(
            geo=dict(bgcolor='black'),
            paper_bgcolor='black',
            plot_bgcolor='black',
            margin=dict(t=0, b=0, l=0, r=0),
            height=500,  # Adjust the height as desired
            width=700,  # Adjust the width as desired
            )              

        st.plotly_chart(fig)


    if tu=='user':
        for i in yr:
            base_path1 = os.path.join(sbase_path, f'{i}')

            sd1=[]
            sd2=[]
            sd3=[]
            sd4=[]


            qt = ['1','2','3','4']
            for j in qt:

                base_path2 = os.path.join(base_path1, f'{j}.json')

                with open(base_path2, 'r') as file:
                    data = file.read()
                    # Parse the JSON data
                    data = json.loads(data)

                    temp1=[]
                    temp2=[]

                    if 'hoverData' in data['data'] :
                        # Iterate over the transaction data and extract relevant details
                        for j in range(len(data['data']['hoverData'])):
                            temp1.append(list(data['data']['hoverData'].keys())[j])
                            temp2.append(list(data['data']['hoverData'].values())[j]['registeredUsers'])
                            
                    sd1.append(temp1)
                    sd2.append(temp2)

                    com.append(pd.DataFrame({'Name': sd1[-1], 'Registered Users': sd2[-1]}))
                

                    if 'hoverData' not in data['data'] :
                        com.append(pd.DataFrame(columns=['Name','Registered Users']))

        uni_name = com[0]['Name'].unique()

        cum_count=[]

        for i in uni_name:
            tem2 = []
            for j in range(0,len(com),4):
                tem1 = 0
                for k in range(4):
                    for s in range(len(uni_name)):
                        if i == com[j+k]['Name'][s]:
                            tem1+= com[j+k]['Registered Users'][s]
                tem2.append(tem1/4)
            cum_count.append(tem2)


        for i in range(len(cum_count)):
            cnt.append(pd.DataFrame(list(zip(yr,cum_count[i]))))

        

        yw=[]
        yrs=['2018','2019','2020','2021','2022']
        for i in yrs:
            tent1=[]
            for j in range(len(cnt)):
                tent=[]
                for s in range(len(cnt[j][0])):
                    if i == cnt[j][0][s]:
                        tent= cnt[j][1][s]
                tent1.append(tent)
            yw.append(tent1)


        yy=[]
        for i in range(len(yw)):
            yy.append(pd.DataFrame(list(zip(uni_name,yw[i]))))


        state_id_map = {}
        for feature in india_states["features"]:
            feature["id"] = feature["properties"]["state_code"]
            state_id_map[feature["properties"]["st_nm"]] = feature["id"]
    
        kpy=100
        opt=['2018','2019','2020','2021','2022']
        year= st.selectbox('Select the year', opt, key=kpy)
        kpy+=1
        i = opt.index(year)

        yy[i][0] = yy[i][0].apply(lambda x:x.replace("&", "and") if('&' in x) else x)

        yy[i][0] = yy[i][0].apply(lambda x: x.title())
        yy[i] = yy[i][yy[i][0]!='Ladakh']
#        st.write(yy) 
        yy[i]["id1"] = yy[i][0].apply(lambda x: state_id_map[x])

        st.header('User - ')
        st.header(f"National Registered Users Data - India - {year}")
        yy[i] = yy[i].rename(columns={1: 'Registered Users'})
        fig = px.choropleth(
            yy[i],
            locations="id1",
            geojson=india_states,
            color='Registered Users',
            hover_name=0,
            hover_data=['Registered Users'],
            color_continuous_scale="Blues", # "Viridis", "YlOrRd", "Blues", 
        )
        fig.update_geos(fitbounds="locations", visible=False)

        # Set the layout properties
        fig.update_layout(
            geo=dict(bgcolor='black'),
            paper_bgcolor='black',
            plot_bgcolor='black',
            margin=dict(t=0, b=0, l=0, r=0),
            height=500,  # Adjust the height as desired
            width=700,  # Adjust the width as desired
)
        st.plotly_chart(fig)






























st.header('Welcome')
ky=1

#oo=['Extract the data','Data Cleaning']
c = st.checkbox('Extract the data')
ky+=1

if c:
    o1 = ['aggregated','map','top']
    o2 = ['transaction','user']
    o3 = ['2018','2019','2020','2021','2022']
    o4=['Country','State']

    c1 = st.selectbox('Select the type of Data to be extracted', o1, key=ky)
    if c1:
        ky+=1
        c2 = st.selectbox('Select the type of Data to be extracted', o2, key=ky+120)
        if c2:
            ky+=1
            c3 = st.selectbox('Select the type of Data to be extracted', o3, key=ky+240)
            if c3:
                c4 = st.selectbox('Select the type of Data to be extracted', o4, key=ky+360)
                get_data_country(c1,c2,c3,c4)


p = st.checkbox('Export to mysql')


if p:
    col1,col2,col3 = st.columns(3)

    with col2:
        p1 = st.checkbox('Export Everything')
    with col3:
        p2 = st.checkbox('Export Selective Data')
    if p1:
        st.warning('It will take atleast 5 minutes to upload all tables to mysql')
        o1 = ['aggregated','map','top']
        o2 = ['transaction','user']
        o3 = ['2018','2019','2020','2021','2022']
        o4 = ['Country','State']
        for c1 in o1:
            for c2 in o2:
                for c3 in o3:
                    for c4 in o4:
                        mysqlimport(c1,c2,c3,c4)    
    if p2:
        o1 = ['aggregated','map','top']
        o2 = ['transaction','user']
        o3 = ['2018','2019','2020','2021','2022']
        o4 = ['Country','State']
        c1 = st.selectbox('Select the type of Data to be extracted', o1, key=ky)
        if c1:
            ky+=1
            c2 = st.selectbox('Select the type of Data to be extracted', o2, key=ky+120)
            if c2:
                ky+=1
                c3 = st.selectbox('Select the type of Data to be extracted', o3, key=ky+240)
                if c3:
                    c4 = st.selectbox('Select the type of Data to be extracted', o4, key=ky+360)
                    smysqlimport(c1,c2,c3,c4)



r = st.checkbox('Display Data from MySql')
if r:
    displaymysql()

r1 = st.checkbox('Delete Data from MySql')
if r1:
    delmysql()

q = st.checkbox('Display Plot')
if q:

    o1 = ['aggregated','map','top']
    o2 = ['transaction','user']

    c1 = st.selectbox('Select the type of Data to be extracted', o1, key=ky)
    if c1:
        ky+=1
        c2 = st.selectbox('Select the type of Data to be extracted', o2, key=ky+120)
        displayplot(c1,c2)

q1 = st.checkbox('Display GeoLive Data')
if q1:
    o2 = ['transaction','user']
    c2 = st.selectbox('Select the type of Data to be extracted', o2, key=ky+120)
    if c2:
        geolive(c2)