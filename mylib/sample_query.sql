SELECT t1.major_category as major, SUM(grad_total) as num_students, SUM(grad_employed) / SUM(grad_total) as employ_rate 
FROM major_delta t1
JOIN students_delta t2 ON t1.major = t2.major
GROUP BY t1.major_category
ORDER BY num_students DESC, major
LIMIT 10;
