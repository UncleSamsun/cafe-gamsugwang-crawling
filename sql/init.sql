CREATE TABLE cafe_ids (
    id BIGINT PRIMARY KEY,
    place_name VARCHAR(255),
    x DECIMAL(20,15),
    y DECIMAL(20,15),
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE cafes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    modified_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    address VARCHAR(255),
    image_url TEXT,
    info VARCHAR(255),
    lat DECIMAL(18,15),
    lon DECIMAL(18,15),
    open_time VARCHAR(512),
    phone_number VARCHAR(255),
    rate DECIMAL(38,2),
    rate_count INT,
    title VARCHAR(255),
    zipcode VARCHAR(255)
);

CREATE TABLE menus (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    modified_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    name VARCHAR(255),
    menu_image_url TEXT,
    price INT,
    modifier VARCHAR(255) DEFAULT 'System',
    cafe_id BIGINT,
    CONSTRAINT fk_menus_cafe_id FOREIGN KEY (cafe_id) REFERENCES cafes(id)
);

CREATE TABLE keywords (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    modified_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    cafe_id BIGINT NOT NULL,
    keyword VARCHAR(255) NOT NULL,
    count INT DEFAULT 1,
    CONSTRAINT fk_keywords_cafe_id FOREIGN KEY (cafe_id) REFERENCES cafes(id),
    UNIQUE KEY uq_cafe_keyword (cafe_id, keyword)
);

CREATE TABLE kakao_reviews (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cafe_id BIGINT,
    content TEXT,
    rating DECIMAL(2,1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE clustered_keywords (
    id INT AUTO_INCREMENT PRIMARY KEY,
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    modified_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    cafe_id VARCHAR(255) NOT NULL,
    cluster_id INT NOT NULL,
    count BIGINT DEFAULT 1,
    keyword VARCHAR(255) NOT NULL
);

CREATE TABLE extracted_keywords (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    modified_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    cafe_id BIGINT NOT NULL,
    keyword VARCHAR(255) NOT NULL,
    count INT DEFAULT 1,
    UNIQUE KEY uq_cafe_keyword_extract (cafe_id, keyword)
);