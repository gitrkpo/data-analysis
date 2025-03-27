WITH CTE AS (
    SELECT *, 
	ROW_NUMBER() OVER (PARTITION BY [Date] ORDER BY [Date]) AS rn
    FROM [dbo].[new_sales_data]
)
SELECT * 
FROM CTE


 
ORDER BY [Date];