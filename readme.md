# Real Estate Data Streaming & Analytics Pipeline

## Project Overview

This project demonstrates an end-to-end **Real Estate Data Streaming & Analytics Pipeline** built with a variety of technologies, including **Python**, **Bright Data**, **Apache Kafka**, **Cassandra**, and **Power BI**. The pipeline is designed to extract real-time property data, process it efficiently, store large datasets, and visualize key insights for market analysis.

### Key Technologies:
- **Python**
- **Bright Data (Web Scraping)**
- **Apache Kafka**
- **Apache Spark**
- **Cassandra**
- **Power BI**

---

## Architecture

![Screenshot 2024-08-29 183426](https://github.com/user-attachments/assets/dc7bdcc8-a4fb-4b73-aa7a-fc85e1c36199)


### Components:
1. **Bright Data (WebSocket Data Extraction)**:
   - Real-time property data is extracted using Bright Data’s WebSocket API.
   
2. **Apache Kafka (Data Streaming)**:
   - The data is streamed in real-time to **Apache Kafka** using a master-worker architecture built with **Docker**.
   
3. **Apache Spark (Data Processing)**:
   - **Apache Spark** processes the streamed data to ensure it's efficiently handled and transformed for storage.
   
4. **Cassandra (Data Storage)**:
   - The processed data is stored in **Cassandra**, a distributed NoSQL database optimized for large-scale real-time data.

5. **Power BI (Visualization)**:
   - **Power BI** is used to create visual dashboards, helping analyze property trends and extract valuable insights from the data.

---

## Features

- **Real-time Data Extraction**: Uses **WebSocket** to continuously gather live property data.
- **Scalable Data Streaming**: Built with **Apache Kafka** for scalable and fault-tolerant data streaming.
- **Big Data Processing**: Real-time data processing using **Apache Spark** to handle large volumes of property data.
- **Efficient Storage**: **Cassandra** is used to store data for fast access and scalability.
- **Market Insights**: Visualize property trends and insights with **Power BI** for decision-making.

---

## Setup Instructions

### Prerequisites

- **Docker**: For containerizing the application components.
- **Kafka**: For streaming real-time data.
- **Cassandra**: For data storage.
- **Power BI**: For data visualization.
- **Python 3.x**: To run the data extraction and processing scripts.

## Visualizations

Here’s an example of the **Power BI** dashboard visualizing key metrics from the real estate data:

![Screenshot 2024-09-26 201745](https://github.com/user-attachments/assets/7d5c8444-896a-43d2-9fd4-b6c88ab2a085)

![Screenshot 2024-09-26 201934](https://github.com/user-attachments/assets/646d01ff-4a96-417e-b64a-28944b602055)


## Future Enhancements

- **Integrate Machine Learning**: Utilize ML models to predict property prices and trends.
- **Expand Data Sources**: Incorporate additional real estate platforms for a more comprehensive dataset.
- **Enhanced Data Cleaning**: Improve data pre-processing to achieve greater accuracy.

---

## License

This project is licensed under the MIT License.

MIT License

Copyright (c) Usman Mahmood <usmanmehmood770@gmail.com>
[MIT.LICENSE.txt](https://github.com/user-attachments/files/17152511/MIT.LICENSE.txt) MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
