ALTER TABLE cleaned_ways ADD COLUMN class_id integer ;
ALTER TABLE cleaned_ways ADD COLUMN type_id integer ;

UPDATE cleaned_ways SET class_id = ways.class_id FROM ways WHERE ways.gid = id;

UPDATE cleaned_ways SET type_id = osm_way_classes.type_id FROM osm_way_classes WHERE cleaned_ways.class_id = osm_way_classes.class_id;

