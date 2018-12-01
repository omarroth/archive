CREATE TABLE public.videos (
    id text NOT NULL,
    finished boolean,
    annotations xml,
    published date
);

ALTER TABLE ONLY public.videos
    ADD CONSTRAINT videos_pkey PRIMARY KEY (id);

GRANT ALL ON TABLE public.videos TO kemal;
