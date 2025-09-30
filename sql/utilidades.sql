CREATE SCHEMA IF NOT EXISTS util;

--Funcion de normalización
CREATE OR REPLACE FUNCTION util.fn_normal_valores(v text)
returns text language sql immutable as $$
    SELECT CASE WHEN v is null then null else upper(btrim(v)) end 
$$;

--Validacion DNI (8 digitos --solo funciona en peru)
CREATE OR REPLACE FUNCTION util.fn_vald_dni(v text)
RETURNS BOOLEAN LANGUAGE SQL immutable AS $$
    SELECT v ~ '^[0-9]{8}$'
$$;

--Convertir a int
CREATE OR REPLACE FUNCTION util.fn_parser_int(v text)
RETURNS INT LANGUAGE plpgsql immutable as $$
begin
    if btrim(v) ~ '^-?[0-9]+$' then
        return btrim(v)::int;
    else
        return null;
    end if;
end;
$$;


-- HASH de lo que coloquemos en el array
CREATE OR REPLACE function util.fn_indice_hash(arr text[])
returns text language sql immutable as $$
    select md5(array_to_string(arr,'||'))
$$;