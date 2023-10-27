import streamlit as st
import pandas as pd
import altair as alt

# Load your data here
joined_df = pd.read_csv("joined_data.csv")

# Streamlit app
def app(joined_df):
    # Title of the app
    st.title('Energy Ratings Distribution by MSOA')

    # Dropdown selector for MSOA
    selected_msoa = st.selectbox('Select MSOA:', options=joined_df['msoa21nm'].unique())

    # Filter the dataframe based on the selected MSOA
    filtered_df = joined_df[joined_df['msoa21nm'] == selected_msoa]

    # Create a histogram of current energy ratings
    chart = alt.Chart(filtered_df).mark_bar().encode(
        x=alt.X('CURRENT_ENERGY_RATING:N', title='Current Energy Rating'),
        y='count()',
        tooltip=['CURRENT_ENERGY_RATING', 'count()']
    ).properties(
        title=f'Distribution of Current Energy Ratings for {selected_msoa}'
    )

    # Display the chart
    st.altair_chart(chart, use_container_width=True)

if __name__ == '__main__':
    app(joined_df)

