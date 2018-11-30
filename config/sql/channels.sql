CREATE TABLE public.channels (
    ucid text NOT NULL,
    finished boolean
);

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_pkey PRIMARY KEY (ucid);

GRANT ALL ON TABLE public.channels TO kemal;
