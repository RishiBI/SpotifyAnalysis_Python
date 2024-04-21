In this project, we leverage Databricks, a powerful platform for large-scale data processing, to conduct a comprehensive analysis of trending songs by artist and year. We utilize a synergistic approach, combining the clarity of SQL queries with the computational power of Python and PySpark. The initial stage involves crafting an SQL query to extract relevant data from your Spotify dataset. This query focuses on capturing crucial information such as artist names, song titles, and the corresponding release years. Subsequently, Python takes center stage. We convert the retrieved data into a PySpark DataFrame, a versatile data structure ideal for distributed processing. By leveraging PySpark's groupBy and count functions, we aggregate the song count for each unique combination of artist and release year. This meticulous process culminates in a PySpark DataFrame that provides a clear picture of artist productivity across different years. To identify trending songs, we employ a carefully defined methodology. This methodology could encompass various techniques, including ranking songs by yearly count, calculating the change in song count compared to previous years, or even incorporating additional data sources for a more holistic analysis. Finally, we can leverage data visualization libraries like Matplotlib or Plotly to transform the results into compelling charts. These visualizations effectively communicate the number of songs released by each artist per year and highlight the trending songs for each period. Through this insightful analysis, we gain valuable insights into artist trends over time, potentially enabling us to predict the next wave of popular music. 
