# Visualizations

### <ins> General Overview </ins>

#### ThoughtSpot

query: ```count Category by Borough for each Category```

#### Tableau
# Process
For the Tableau visualizations we created 3 maps to visualize the data aggreagation of both the taxi zones and the landmarks. 

The first map was just a general outline of where the boroughs and taxi zones are. This visualization also includes the landmarks mapped out on this layer. With this visualization we were able to see the landmark denisty within the boroughs, with Manhattan being the borough having the most landmarks. 

The second map was a visualization to display the traffic congestion of the taxi zones. This was based on the sum of the taxi and high volume for hire vehicles rides for each zones. Based on this visualization you are able to see that Manhattan is the borough with the most traffic congestion because the taxi zones are more darker compared to the zones in the other boroughs. In relation to the first visualization you can notice there is a positve correlation between traffic congestion and landmark density. The main example being Manhattan having a larger density and traffic congestion in both scenarios. We also noticed that there were 2 taxi zones that had a high traffic congestion and were outside of the Manhattan borough. These zones were JFK Airport and LaGuardia Airport which makes sense because these zones are airport zones and there are a lot of people traveling to and from this zone using taxis and high volume for hire vehicles.

```
SELECT T.DO_ZONE, C + D as Total FROM 
(SELECT COUNT(DO_LOCATIONID) AS C, DO_ZONE
FROM CAPSTONE_DE.GROUP_1.TAXI
GROUP BY DO_ZONE) AS T JOIN 

(SELECT COUNT(DO_LOCATIONID) as D, DO_ZONE
FROM CAPSTONE_DE.GROUP_1.HVFHV
GROUP BY DO_ZONE) AS H ON T.DO_ZONE = H.DO_ZONE;
```

Our third map was the interactive dashboard
