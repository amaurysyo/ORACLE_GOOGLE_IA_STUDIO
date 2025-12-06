SELECT * FROM oraculo.slice_events 
WHERE event_time BETWEEN '2025-12-03 18:00' AND '2025-12-04 16:00' 
AND  fields ? 'k' 
  AND fields ->>'mode' = 'hitting'
ORDER BY id ASC, event_time ASC 