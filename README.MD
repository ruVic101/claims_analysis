## Question 1: Health & Life Insurance Portfolio Analysis

**User Story:**
As an actuarial analyst, I want to analyze our Health and Life insurance claim patterns across different customer segments and regions, so that I can identify trends and support our pricing strategy decisions.

**Goal:**
Provide insights into claim behavior for our Health and Life products across various customer tiers and geographic locations.

**Acceptance Criteria:**
- Analysis covers Health and Life products only
- Results are broken down by customer tier and country
- Report shows claim counts, amounts, and claim approval rate
- All customer segments are visible

---

# **Solution**
---

### **Approach**
First narrow down the claims data to includ only Health and Life Insurance Products. This ensures the alignment of the solution to the requirement of the Actuarial Analyst. Then, the filtered claims data was joined with customer data to identify customer's tier and country. This join has been designed to include all the customers irrespective of the claims.

From there, I grouped the data by customer tier and country. Within each group I have calculated five key metrics:
 - Total Claim count - the total number of claims filed.
 - Approved Claim count - The total number of claims which were approved.
 - Claim amount - The sum of claims value.
 - Approved Claim amount - The sum of claim amount that was approved.
 - Approval rate - The ratio of approved claims to total claims.

This provides key insights into the claim behaviour across different customer segments (tier) and regions covering Health and Life Products.

*For Example,*
- It can be seen that the claim settlement ratio remains in the bracket of 32-35% across different tiers and countries - This indicates fairly consistent approval process across diff regions.
- Claim settlement ratio in FR and NO are consistently higher than other countries across all customer tiers.

---

## Question 2: Property & Casualty Performance Report

**User Story:**
As a product manager for Auto and Home insurance, I need to understand how our products are performing across different markets and risk profiles, so that I can decide where to focus our operational improvements.

**Goal:**
Create a comprehensive view of claim activity for our property and casualty business, showing patterns by location, risk level, and claim characteristics.

**Acceptance Criteria:**
- Report includes non life products
- Data is segmented by country and risks
- Shows claim volumes and financial metrics
- Product information reflects historical changes (products have version histories)

---

# **Solution**

### **Approach**

For the Property and Casualty performance report, the focus shifts towards Home and Auto insurance products. For this report, I filtered the product dataset for Auto and Home products only. 

Next, I joined the claims data with products to enrich the dataset with key product metrics. Since the products can have different versions, I have implemeted the solution to associate a claim only with the product which was active at the time of the claim. The final dataset groups the data on **product type**, **country**, **risk category** and **product version** and with each group, I calculated:
 - Total Claim count - The total number of claims filed.
 - Approved Claim count - The total number of claims which were approved.
 - Claim amount - The sum of claims value.
 - Approved Claim amount - The sum of claim amount that was approved.
 - Approval rate - The ratio of approved claims to total claims.
 - Avg claim amount - Its the avg amount that was claimed per claim (includes claims with status -> Approve, Pending, Rejected)
 - Avg permium (for that specific product version) - The premium that was charged to the customer for the corresponding product version during the claim period. This will be useful while analyzing if the premium of a certain product needs revision.
 - Commission Rate - The commission Rate that was offered for associated product version.
 - product status (ACTIVE/INACTIVE) - This shows if the product version which was active during the claim, is still active today.

The key data points we get in this analysis will be instrumental in understanding how the products are performing across different countries and risk profiles.