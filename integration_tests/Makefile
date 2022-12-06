test-redshift:
	dbt deps
	dbt build --target redshift --full-refresh

test-postgres:
	dbt deps
	dbt build --target postgres --full-refresh

clean:
	dbt clean
