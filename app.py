import os
import json
import pandas as pd
import pymysql
import streamlit as st
import plotly.express as px
import requests


connection = pymysql.connect(
        host = 'localhost', user = 'root',
         password = '12345',database='phonepay')
print("connection = ", connection)
cursor = connection.cursor()
print("cursor = ", cursor)

#Bussiness case studies:query 1: Top 10 states have highest transaction count

def get_connection():
    return  pymysql.connect(
        host = 'localhost', user = 'root',
         password = '12345',database='phonepay')
print("connection = ", connection)
cursor = connection.cursor()
print("cursor = ", cursor)
Query1=cursor.execute("""
    SELECT 
        state, 
        SUM(Transaction_count) AS Total_Transaction_count
    FROM agg_trans 
    GROUP BY state
    ORDER BY Total_Transaction_count  DESC
    LIMIT 10;
""")
rows=cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df1 = pd.DataFrame(rows, columns=columns)
connection.commit()
#Bussiness case studies:query 2: Top 10 states have lowest transaction count
Query2=cursor.execute("""
    SELECT 
        state, 
        SUM(Transaction_count) AS Total_Transaction_count
    FROM agg_trans 
    GROUP BY state
    ORDER BY Total_Transaction_count ASC
    LIMIT 10;
""")
rows=cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df2= pd.DataFrame(rows, columns=columns)
connection.commit()
#Bussiness case studies:query 3: Top 10 states have Highest transaction Amount
Query3=cursor.execute("""
    SELECT 
        state, 
        SUM(Transaction_count) AS Total_Transaction_Amount
    FROM agg_trans 
    GROUP BY state
    ORDER BY Total_Transaction_Amount  DESC
    LIMIT 10;
""")
rows=cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df3= pd.DataFrame(rows, columns=columns)
connection.commit()
#Bussiness case studies:query 4: Top 10 states have lowest transaction Amount
Query4=cursor.execute("""
    SELECT 
        state, 
        SUM(Transaction_count) AS Total_Transaction_Amount
    FROM agg_trans 
    GROUP BY state
    ORDER BY Total_Transaction_Amount  ASC
    LIMIT 10;
""")
rows=cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df4= pd.DataFrame(rows, columns=columns)
connection.commit()
#Bussiness case studies:query 5: Top 10 DISTRICT have HIGHEST RegisteredUsers:
Query5=cursor.execute("""

      SELECT DISTINCT
        district,
        SUM(RegisteredUsers) As Total_RegisteredUsers
    FROM Map_Users
    GROUP BY district
    ORDER BY Total_RegisteredUsers DESC
    LIMIT 10;
""")
rows=cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df5= pd.DataFrame(rows, columns=columns)
connection.commit()
#Bussiness case studies:query 6: Top 10 DISTRICT have Lowest RegisteredUsers:
Query6=cursor.execute("""

      SELECT
        district,
        SUM(RegisteredUsers) As Total_RegisteredUsers
    FROM Map_Users
    GROUP BY district
    ORDER BY Total_RegisteredUsers ASC
    LIMIT 10;
""")
rows=cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df6= pd.DataFrame(rows, columns=columns)
connection.commit()
#Bussiness case studies:query 7: Top 10 STATE,BRAND WITH QUATER have HIGHEST RegisteredUsers:
Query7=cursor.execute("""
     SELECT
    brand,
    SUM(Registered_users_count) AS Total_Registered_users_count
FROM agg_users
GROUP BY brand
ORDER BY Total_Registered_users_count DESC
LIMIT 10;
""")  
rows=cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df7= pd.DataFrame(rows, columns=columns)
connection.commit() 
#Bussiness case studies:query 8:Top 10 state have HIGHEST INSURANCE AMOUNT with YEAR and Quater:

Query8=cursor.execute("""
          SELECT
               state,
               SUM(insurance_Amount) As Total_insurance_Amount
               FROM Top_ins
               group by state
               order by Total_insurance_Amount DESC
               LIMIT 10;
""")
rows=cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df8= pd.DataFrame(rows, columns=columns)
connection.commit()
#Bussiness case studies:query 9:Top 10 state have LOWEST INSURANCE COUNT with YEAR and Quater:
Query9=cursor.execute("""
          SELECT
               state,
               SUM(insurance_count) As Total_insurance_count
               FROM map_ins
               group by state
               order by Total_insurance_count DESC
               LIMIT 10;
""")
rows=cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df9= pd.DataFrame(rows, columns=columns)
connection.commit()
#Bussiness case studies:query 10:Top 10 state have HIGHEST AppOPens COUNT with districts and registeredusers:
Query10=cursor.execute("""
        SELECT
    district,
    SUM(registeredUsers) AS Total_RegisteredUsers,
    SUM(AppOpens) AS Total_AppOpens
FROM map_users
GROUP BY district
ORDER BY Total_AppOpens DESC
LIMIT 10;
""")
rows=cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df10= pd.DataFrame(rows, columns=columns)
connection.commit()

#Bussiness case studies:query 12:Top 10 state have HIGHEST AppOPens COUNT with state and registeredusers:
Query11=cursor.execute("""
         SELECT
    state,
    SUM(registeredUsers) AS Total_RegisteredUsers,
    SUM(AppOpens) AS Total_AppOpens
FROM map_users
GROUP BY state
ORDER BY Total_AppOpens DESC
LIMIT 10;
""")
rows=cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df11= pd.DataFrame(rows, columns=columns)
connection.commit()
# Bussiness case studies: query 13:Transaction Type with highest transaction count:
Query13=cursor.execute("""
    SELECT 
        Transaction_type,
        SUM(Transaction_count) AS Total_Transaction_count
    FROM agg_trans 
    GROUP BY Transaction_type
    ORDER BY Total_Transaction_count DESC
    LIMIT 4;
""")
rows=cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df12= pd.DataFrame(rows, columns=columns)
connection.close()


#This is to direct the path to get the data as states
path="C:\\Users\\Home\\Desktop\\phonepay\\data\\aggregated\\transaction\\country\\india\\state\\"
Agg_state_list=os.listdir(path)
Agg_state_list

#Dataframe1=Aggregated_Transaction
#This is to extract the data's to create a dataframe--Aggregated_Transaction:

clm={'State':[], 'Year':[],'Quater':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}

for i in Agg_state_list:
    p_i=path+i+"/"
    print(p_i)
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              clm['Transaction_type'].append(Name)
              clm['Transaction_count'].append(count)
              clm['Transaction_amount'].append(amount)
              clm['State'].append(i)
              clm['Year'].append(j)
              clm['Quater'].append(int(k.strip('.json')))
              #Succesfully created a dataframe
Agg_Trans=pd.DataFrame(clm)
state_name_map = {
    "andaman-&-nicobar-islands": "Andaman & Nicobar Island",
    "andhra-pradesh": "Andhra Pradesh",
    "arunachal-pradesh": "Arunanchal Pradesh", 
    "assam": "Assam",
    "bihar": "Bihar",
    "chandigarh": "Chandigarh",
    "chhattisgarh": "Chhattisgarh",
    "dadra-&-nagar-haveli-&-daman-&-diu": "Dadra & Nagar Haveli & Daman & Diu",
    "delhi": "NCT of Delhi",
    "goa": "Goa",
    "gujarat": "Gujarat",
    "haryana": "Haryana",
    "himachal-pradesh": "Himachal Pradesh",
    "jammu-&-kashmir": "Jammu & Kashmir",
    "jharkhand": "Jharkhand",
    "karnataka": "Karnataka",
    "kerala": "Kerala",
    "ladakh": "Ladakh",
    "lakshadweep": "Lakshadweep",
    "madhya-pradesh": "Madhya Pradesh",
    "maharashtra": "Maharashtra",
    "manipur": "Manipur",
    "meghalaya": "Meghalaya",
    "mizoram": "Mizoram",
    "nagaland": "Nagaland",
    "odisha": "Odisha",
    "puducherry": "Puducherry",
    "punjab": "Punjab",
    "rajasthan": "Rajasthan",
    "sikkim": "Sikkim",
    "tamil-nadu": "Tamil Nadu",
    "telangana": "Telangana",
    "tripura": "Tripura",
    "uttar-pradesh": "Uttar Pradesh",
    "uttarakhand": "Uttarakhand",
    "west-bengal": "West Bengal"
}





#streamlit part:
import streamlit as st
st.title("📱PhonePe Transaction Insights")




page=st.sidebar.radio("Navigation",["Home","Insights"])
if page=="Home":
    st.write("All India Transaction")
    
    Agg_Trans["State"] = Agg_Trans["State"].str.strip().str.lower()
    Agg_Trans["State"] = Agg_Trans["State"].replace(state_name_map)
   
    geo_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    geojson_data = requests.get(geo_url).json()
    year_selected = st.selectbox("Select Year", sorted(Agg_Trans["Year"].unique()))
    quarter_selected = st.selectbox("Select Quarter", sorted(Agg_Trans["Quater"].unique()))
    metric_selected = st.radio(
    "Select Metric", 
    ["Transaction_count", "Transaction_amount"],
    index=0
    )
    
    df_filtered = Agg_Trans[
    (Agg_Trans["Year"] == year_selected) &
    (Agg_Trans["Quater"] == quarter_selected)]
 # Group by state for map
    df_grouped = df_filtered.groupby("State").agg({
    "Transaction_count": "sum",
    "Transaction_amount": "sum"
    }).reset_index()

# Group by transaction type for side display
    df_type = df_filtered.groupby("Transaction_type").agg({
        "Transaction_count": "sum",
        "Transaction_amount": "sum"
    }).reset_index()
    col1, col2 = st.columns([4,1])
    

   
    
   

# Display KPIs above the map
    with col1:
      total_count = df_grouped["Transaction_count"].sum()
      total_amount = df_grouped["Transaction_amount"].sum()
      kpi1,kpi2=st.columns(2)
      kpi1.metric(
      label=f"Total Count ({year_selected} Q{quarter_selected})",
      value=f"{total_count:,.0f}"
      )
      kpi2.metric(
      label=f"Total Amount ({year_selected} Q{quarter_selected})",
      value=f"₹{total_amount:,.0f}"
      )

    #Map
      fig = px.choropleth(
      df_grouped,
      geojson=geojson_data,
      featureidkey="properties.ST_NM",
      locations="State",
      color=metric_selected,
      color_continuous_scale="YlOrRd",
      range_color=(
        df_grouped[metric_selected].min(),
        df_grouped[metric_selected].max()
      ),
      hover_name="State",
      hover_data={
        "Transaction_count": True,
        "Transaction_amount": True
      }
      )

      fig.update_geos(fitbounds="locations", visible=False)
      st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.write("### Transaction Type Share")
        fig_pie = px.pie(
        df_type,
        names="Transaction_type",
        values="Transaction_amount",  # or "Transaction_count"
        title="Share by Amount",
        hole=0.3,
        color_discrete_sequence=px.colors.qualitative.Set2
        )
        fig_pie.update_traces(textinfo="percent+label")
        st.plotly_chart(fig_pie, use_container_width=True)
  

elif page=="Insights":
    st.write("PhonePe Business Case Studies")
    sb=st.selectbox("Select a Bussiness case Study",["1.Decoding Transaction Dynamics on PhonePe","2.User Registration Analysis","3.Insurance Transactions Analysis","4.Device Dominance and User Engagement Analysis","5.Transaction Analysis Across States"])
    st.write(sb)

    if sb=="1.Decoding Transaction Dynamics on PhonePe":
      st.write("Top 10 States of Transaction")

      st.dataframe(df1)
      fig=px.bar(df1,x='state',y='Total_Transaction_count',color='Total_Transaction_count',title='Top 10 states of  Highest transaction count')
      st.plotly_chart(fig)
      st.dataframe(df2)
      fig=px.bar(df2,x='Total_Transaction_count',y='state',color='Total_Transaction_count', orientation="h",title='Top 10 states of  Lowest transaction count')
      fig.update_traces(textposition="outside")
      st.plotly_chart(fig, use_container_width=True)
      
      

    elif sb=="2.User Registration Analysis":
        st.dataframe(df5)
        fig=px.pie(df5,names='district',values='Total_RegisteredUsers',title='Top 10 district of highest registered users')
        st.plotly_chart(fig)
        st.dataframe(df7)
        fig=px.bar(df7,x='brand',y='Total_Registered_users_count',color='Total_Registered_users_count',title='Top 10 district of highest brand registered users')
        st.plotly_chart(fig)
       
    elif sb=="3.Insurance Transactions Analysis":
        st.dataframe(df8)
        fig=px.bar(df8,x='state',y='Total_insurance_Amount',color='Total_insurance_Amount',title='Top states of Highest Insurance Amount')
        st.plotly_chart(fig)
        st.dataframe(df9)
        fig=px.pie(df9,names='state',values='Total_insurance_count',hole=0.5,title='Top states of Highest Insurance count')
        st.plotly_chart(fig)
        

    elif sb=="4.Device Dominance and User Engagement Analysis":
         st.dataframe(df10)
         fig=px.line(df10,x='district',y='Total_RegisteredUsers',color='Total_AppOpens',markers=True,title='Top 10 district AppOpens')
         st.plotly_chart(fig)
         st.dataframe(df11)
         fig=px.pie(df11,names='state',values='Total_RegisteredUsers',color='Total_AppOpens',title='Top 10 state AppOpens')
         st.plotly_chart(fig)


    elif sb=="5.Transaction Analysis Across States":
        st.dataframe(df3)
        fig=px.pie(df3,names='state',values='Total_Transaction_Amount',title='Top 10 states of  Highest transaction Amount')
        st.plotly_chart(fig)

        st.dataframe(df4)
        fig=px.pie(df4,names='state',values='Total_Transaction_Amount',hole=0.3,title='Top 10 states of  Lowest transaction Amount')
        st.plotly_chart(fig)

 



