SET TIME ZONE TO `America/Santiago`;
CREATE IF NOT EXISTS TABLE `miembros` (
    `nombre` VARCHAR(50) NOT NULL,
    `apellido` VARCHAR(50) NOT NULL,
    `correo` VARCHAR(50) NOT NULL,
    `patente` VARCHAR(50) NOT NULL,
    `rut` VARCHAR(50) NOT NULL,
    `premium` BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (`rut`),
    FOREIGN KEY (`patente`) REFERENCES `ventas`(`patente`)
);
CREATE IF NOT EXISTS TABLE `ventas` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `patente` VARCHAR(50) NOT NULL,
    `cliente` VARCHAR(50) NOT NULL,
    `cantidad` INT NOT NULL,
    `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `ubicacion` VARCHAR(50) NOT NULL,
    PRIMARY KEY (`id`)
);