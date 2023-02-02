CREATE SCHEMA api;
ALTER SCHEMA api OWNER TO postgres;

GRANT USAGE ON SCHEMA api TO postgres;

/* CREATE INDEX idx_arlie_river_km_id_datetime_cloud_perc ON arlie (river_km_id, datetime, cloud_perc); */
/* CREATE INDEX idx_river_basins_st_transform_geometry ON river_basins USING gist (ST_Transform(ST_SetSRID(geometry, 3035), 4326)); */
/* CREATE INDEX idx_river_basins_eu_hydro_id ON river_basins (eu_hydro_id); */
ALTER user postgres set work_mem='32GB';
DROP FUNCTION transform_type(text);
CREATE OR REPLACE FUNCTION transform_type(sat_type text)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
type_int integer;
BEGIN
	CASE sat_type
	  WHEN 'Sentinel-1' THEN
	    type_int = 1;
	  WHEN 'Sentinel-2' THEN
	    type_int = 2;
	  WHEN 'Sentinel-1 Sentinel-2' THEN
	    type_int = 0;
	  ELSE
	    type_int = 999;
	END CASE;
	RETURN type_int;

end;

$function$
;


DROP FUNCTION api.get_arlie(text, text, text, text, int, boolean);
CREATE OR REPLACE FUNCTION api.get_arlie(startdate text, completiondate text, geometrywkt text DEFAULT 'POLYGON()', euhydroid text DEFAULT 'NONE', cloudcoveragemax int DEFAULT 100, getonlysize boolean DEFAULT FALSE)
 RETURNS TABLE(j integer[])
 LANGUAGE plpgsql
AS $function$
DECLARE ids integer[];
BEGIN
	IF euhydroid <> 'NONE' and geometrywkt <> 'POLYGON()' THEN
		raise exception 'You must specify geometrywkt or euhydroid';
	END IF;

	IF euhydroid <> 'NONE' THEN
		SELECT array_agg(id) into ids
		FROM river_basins 
		WHERE eu_hydro_id = euhydroid;
	ELSIF geometrywkt <> 'POLYGON()' THEN
		SELECT array_agg(id) into ids
		FROM river_basins 
		WHERE ST_Intersects(ST_Transform(ST_SetSRID(geometry, 3035), 4326), ST_SetSRID(ST_GeomFromText(geometrywkt), 4326));
	ELSE
		raise exception 'You must specify geometrywkt or euhydroid';
	END IF;

	IF array_length(ids, 1) > 70000 AND NOT getonlysize THEN
		raise exception 'The requested area specified is too large, please change for a request on a smaller area. % polygons requested, the limit is 70000 polygons.', array_length(ids, 1);
	END IF;

	IF getonlysize THEN
		RETURN QUERY
		SELECT ARRAY[COUNT(*)::integer]
		FROM arlie
		WHERE river_km_id = ANY (ids::int[]) and datetime >= startdate::timestamp AND datetime <= completiondate::timestamp and cloud_perc <= cloudcoveragemax;
	ELSE
		RETURN QUERY
		SELECT ARRAY[river_km_id, to_char(datetime, 'YYYYMMDD')::integer, to_char(datetime, 'HH24MISS')::integer, water_perc, ice_perc, other_perc, cloud_perc, nd_perc, qc, transform_type(type)]
		FROM arlie 
		WHERE river_km_id = ANY (ids::int[]) and datetime >= startdate::timestamp AND datetime <= completiondate::timestamp and cloud_perc <= cloudcoveragemax;
	END IF;

end;

$function$
;



DROP FUNCTION api.get_geometries(text, boolean);

CREATE OR REPLACE FUNCTION api.get_geometries(geometrywkt text, getonlyids boolean DEFAULT FALSE)
 RETURNS TABLE(j text[])
 LANGUAGE plpgsql
AS $function$
DECLARE ids integer[];
BEGIN

	SELECT array_agg(id) into ids
	FROM river_basins 
	WHERE ST_Intersects(ST_Transform(ST_SetSRID(geometry, 3035), 4326), ST_SetSRID(ST_GeomFromText(geometrywkt), 4326));

	IF array_length(ids, 1) > 70000 AND NOT getonlyids THEN
		raise exception 'The requested area specified is too large, please change for a request on a smaller area. % polygons requested, the limit is 70000 polygons.', array_length(ids, 1);
	END IF;

	IF getonlyids THEN
		RETURN QUERY SELECT ARRAY[id::text]
		FROM river_basins 
		WHERE ST_Intersects(ST_Transform(ST_SetSRID(geometry, 3035), 4326), ST_SetSRID(ST_GeomFromText(geometrywkt), 4326));
	ELSE
		RETURN QUERY SELECT ARRAY[id::text, ST_AsText(ST_Force2D(geometry), 5), basin_name::text, eu_hydro_id::text, object_nam::text, TRUNC(area::numeric, 2)::text, river_km::text]
		FROM river_basins 
		WHERE ST_Intersects(ST_Transform(ST_SetSRID(geometry, 3035), 4326), ST_SetSRID(ST_GeomFromText(geometrywkt), 4326));
	END IF;

end;

$function$
;


DROP FUNCTION api.get_geometries_by_id(text);

CREATE OR REPLACE FUNCTION api.get_geometries_by_id(ids text)
 RETURNS TABLE(j text[])
 LANGUAGE plpgsql
AS $function$
DECLARE
       ids_int INT[];
BEGIN
       ids_int = string_to_array(ids,',');

	RETURN QUERY SELECT ARRAY[id::text, ST_AsText(ST_Force2D(geometry), 5), basin_name::text, eu_hydro_id::text, object_nam::text, TRUNC(area::numeric, 2)::text, river_km::text]
	FROM river_basins 
	WHERE id = ANY(ids_int::int[]);

end;

$function$
;
