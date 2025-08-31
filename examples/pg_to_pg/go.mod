module github.com/shogotsuneto/go-simple-es-projector/examples/pg_to_pg

go 1.24.6

require (
	github.com/lib/pq v1.10.9
	github.com/shogotsuneto/go-simple-es-projector v0.0.0
	github.com/shogotsuneto/go-simple-eventstore v0.0.8
)

replace github.com/shogotsuneto/go-simple-es-projector => ../..
