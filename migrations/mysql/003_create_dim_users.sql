-- =====================================================
-- MOCK API USER DIMENSION
-- Provides the /users endpoint with customer data in MySQL.
-- =====================================================

USE ecommerce_staging;

CREATE TABLE IF NOT EXISTS dim_users (
    user_id     BIGINT PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    city        VARCHAR(100),
    country     VARCHAR(100),
    segment     VARCHAR(50) NOT NULL,
    created_at  DATETIME NOT NULL,
    updated_at  DATETIME NULL
);

CREATE INDEX idx_dim_users_email ON dim_users(email);
CREATE INDEX idx_dim_users_segment ON dim_users(segment);
CREATE INDEX idx_dim_users_updated_at ON dim_users(updated_at);

