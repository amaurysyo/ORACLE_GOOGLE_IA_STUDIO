--
-- TOC entry 19 (class 1247 OID 20000001)
-- Name: instrument_id_t; Type: DOMAIN; Schema: public; Owner: -
--

CREATE DOMAIN public.instrument_id_t AS text;


--
-- TOC entry 20 (class 1247 OID 20000002)
-- Name: side_t; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.side_t AS ENUM (
    'buy',
    'sell'
);


--
-- TOC entry 21 (class 1247 OID 20000003)
-- Name: action_t; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.action_t AS ENUM (
    'insert',
    'update',
    'delete'
);


--
-- TOC entry 22 (class 1247 OID 20000004)
-- Name: severity_t; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.severity_t AS ENUM (
    'ALTA',
    'MEDIA',
    'BAJA'
);


