

# Project Overview: EnodeB Performance Monitoring During Vendor Transition
The full project documentation is hosted here:  
👉 [View Detailed Documentation](https://github.com/GeneSUN/Class_SNAP)




## 1. Background & Objective
As part of the nationwide transition of station towers from **Vendor A** to **Vendor B**, the goal of this project is to **monitor performance changes** before and after the vendor switch.  

<img width="609" height="1180" alt="image" src="https://github.com/user-attachments/assets/dde34ec3-e42a-4144-87ef-a58d559da783" />


The system detects whether each tower experiences performance degradation following the transition.

---

## 2. Hierarchical Monitoring Framework
Over **40,000 towers** nationwide, with around **30 transitions per day**.
The monitoring system follows a **multi-level architecture**:

- **Tower Level (Building):** Represents the overall performance of a station tower.  
- **Sector Level (Floor):** Each tower consists of multiple sectors representing directional coverage.  
- **Carrier Level (Room):** Each sector supports multiple carriers, providing the most granular performance view.

<p align="center">
  <img src="https://github.com/user-attachments/assets/ed032f38-094b-4292-93b8-ba1a4a2e208d" width="480" />
  <img width="300" height="500" alt="image" src="https://github.com/user-attachments/assets/297a68ec-9b29-4135-8ffd-ca6114d57561" />

</p>



The project began with **tower-level monitoring** and will progressively expand to **sector** and **carrier levels** for more detailed diagnostics.

### 📊 Feature Diversity
Each tower has numerous KPIs with unique patterns, making it difficult to define a universal threshold or standard.

    fsm_column_mapping = { 
        "FSM_LTE_DataERABDrop%": "bearer_drop_rate", 
        "FSM_LTE_AvgRRCUsers": "avgconnected_users", 
        "FSM_LTE_DLRLCMBytes": "dl_data_volume", 
        "FSM_LTE_DL_BurstTputMbps": "uptp_user_perceived_throughput", 
        "S1U_SIP_SC_CallDrop%": "sip_dc_rate", 
        "FSM_LTE_RRCF%": "rrc_setup_failure",
        "RTP_Gap_Duration_Ratio_Avg%": "rtp_gap"
    } 

---

## 3. System Purpose & Interpretability

This project serves a **dual role**:

- **📊 Dashboard & Analytic Tools:**  
  Provides interpretable visualizations and summaries to help **electrical engineers** validate their judgments and investigate tower issues.
- **🚨 Anomaly Detection:**  
  Automatically identifies performance degradation or anomalies after the vendor transition.  

The dashboard not only highlights abnormal towers or sectors but also integrates contextual data such as **tickets** and **KPIs**, enabling engineers to cross-validate network health with field operations.

---


## 4. Key Challenges


### 🌎 Strong Spatial Heterogeneity
Towers are located in diverse regions with distinct environmental and network conditions, making it impractical to build one global model for performance monitoring.

### 🔗 Data Complexity
Multiple data sources must be integrated, including KPIs, alarms, tickets, and configuration data.
https://github.com/GeneSUN/Class_SNAP/blob/main/SNAP-Map.png

### ⚡ Scalability & Performance
The pipeline operates in **near real-time**, leveraging **parallel processing** and **distributed computing (Spark)** for data ingestion, preprocessing, and calculation.

### 🚀 Urgent Delivery

---

## 5. Technical Implementation

- **Data Pipeline:** End-to-end system covering **data extraction**, **cleaning**, **transformation**, and **feature computation**.
<img width="2642" height="1418" alt="image" src="https://github.com/user-attachments/assets/1978d09b-7969-4b2f-b586-833b77eb9faa" />


- **Scalability:** Utilizes **Apache Spark** and **parallel processing** for large-scale efficiency.
  
- **Modeling Approach:** Each tower is monitored based on its **own seasonal historical behavior**, ensuring localized and adaptive tracking.  
    <img src="https://miro.medium.com/v2/resize:fit:2000/format:webp/1*k4vlO1vRKuxss84Ren88ZA.png" width="800" alt="Hierarchy Diagram">
    
  _This [article](https://medium.com/@injure21/transform-time-series-data-for-supervised-learning-from-sequence-to-samples-a7b12306b077) talks more detail of the method_

- **Visualization:**  
  - Each **tower**, **sector**, and **carrier** can be explored across **multiple KPIs** simultaneously using **overlayed trend lines**, **correlation matrices**, and **heatmaps**.  
  - Temporal patterns such as **daily cycles, seasonal shifts, and abrupt deviations** are clearly displayed, enabling engineers to visually identify relationships among metrics and pinpoint when and how anomalies emerge.

<p align="center">
  <img width="400" height="300" alt="image" src="https://github.com/user-attachments/assets/bbcc1495-1283-40a0-81aa-2c06e9dbc316" />
  <img width="400" height="300" alt="image" src="https://github.com/user-attachments/assets/5119d585-76b5-4b7b-abf4-bd5e729e2797" />
</p>

- **Extensibility:** Supports **multi-level granularity** and easy integration of new data sources or metrics.

---

## 6. Solution:

- https://github.com/GeneSUN/Class_SNAP/blob/main/class_SNAP.py

<img width="2886" height="1842" alt="image" src="https://github.com/user-attachments/assets/ceec821b-8dad-44a2-a9eb-e28e4aaca2b0" />


- https://github.com/GeneSUN/Class_SNAP/tree/main
