![Github](https://github.com/user-attachments/assets/731bfbcd-d2a3-430c-87b2-4e576c927503)

# Project Overview
This project aims to analyze Wikipedia's Clickstream dataset, which records user navigation patterns between Wikipedia articles. The dataset contains click data between pairs of Wikipedia pages, including both internal links and external sources such as search engines. By analyzing this data, we can gain insights into user behavior, identify trends in how readers move between topics, and understand the role of external search engines in driving traffic to Wikipedia.

# Project Goals
- Analyze user navigation patterns between Wikipedia articles.
- Examine the role of external searches and internal links in guiding users to specific topics.
- Visualize the click distributions between articles and external sources.

The data used in this analysis comes from Wikipedia's open-source Clickstream dataset, which records millions of user interactions.

## Data Loading
The project leverages Apache Spark to handle large datasets efficiently. We load and inspect the full clickstream dataset, which is provided as a TSV (Tab-Separated Values) file.

## Data Analysis
We filter the data to analyze the clickstream leading to the "Hanging Gardens of Babylon" page. This gives us the top sources contributing to clicks on the "Hanging Gardens of Babylon" page, revealing that external search engines play a major role.
```
clickstream.filter(clickstream.target_page == 'Hanging_Gardens_of_Babylon')\
    .orderBy('click_count', ascending=False)\
    .show()
```

Next, calculate the total number of clicks grouped by link category to understand the distribution of external vs. internal sources.Results show that external links contribute significantly more clicks compared to internal Wikipedia links, highlighting the importance of search engines in driving traffic.
```
clickstream.groupBy('link_category')\
    .sum('click_count')\
    .show(5)
```

## Saving Results to Disk
We create a DataFrame that contains only article-to-article internal links and save the results in both CSV and Parquet formats.
```
internal_clickstream = clickstream\
    .select(["source_page", "target_page", "click_count"])\
    .filter(clickstream.link_category == 'link')

# Save results as CSV and Parquet
internal_clickstream.write.csv('./results/article_links_csv/', mode="overwrite")
internal_clickstream.write.parquet('./results/article_links_parquet/', mode="overwrite")
```

# Conclusion
This project provided a comprehensive analysis of Wikipedia's clickstream data, revealing:
* External search engines are a major driver of traffic to Wikipedia.
* Internal Wikipedia links also contribute significantly to article navigation, particularly within related topics.
* By filtering and aggregating the data, we were able to extract meaningful insights about how users interact with Wikipedia.
Through this project, we gain insights into not just user behavior on Wikipedia but also the practical application of big data technologies in uncovering patterns and trends within vast datasets.

# Further Research
* Temporal Analysis: Investigate how click patterns change over time.
* Topic-Specific Clickstreams: Examine clickstreams for specific subject areas, such as science or history.
* User Behavior Analysis: Use additional datasets to explore user behavior across different Wikipedia versions (e.g., by language or region).
