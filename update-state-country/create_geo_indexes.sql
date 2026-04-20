-- Create trigram indexes for geo_countries foreign lookups
-- Significantly speeds up Phase 2 country resolution

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_geo_countries_state_trgm
  ON geo_countries USING gist(state_name gist_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_geo_countries_country_trgm
  ON geo_countries USING gist(country_name gist_trgm_ops);

-- Also add B-tree indexes for equality lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_geo_countries_state
  ON geo_countries(state_name);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_geo_countries_country
  ON geo_countries(country_name);

-- Analyze to update statistics
ANALYZE geo_countries;
