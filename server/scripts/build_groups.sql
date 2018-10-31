
insert into geodata_group32 
select avg(latitude) as latitude, avg(longitude) as longitude, count(*) as nb_points
  from geodata
  group by round(geodata.longitude*32), round(geodata.latitude*32);

insert into geodata_group256 
select avg(latitude) as latitude, avg(longitude) as longitude, count(*) as nb_points
  from geodata
  group by round(geodata.longitude*256), round(geodata.latitude*256);

insert into geodata_group2048 
select avg(latitude) as latitude, avg(longitude) as longitude, count(*) as nb_points
  from geodata
  group by round(geodata.longitude*2048), round(geodata.latitude*2048);

