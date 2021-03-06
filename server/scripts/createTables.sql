DROP TABLE dataset;
DROP TABLE geodata;

--
-- Structure de la table `dataset`
--

CREATE TABLE IF NOT EXISTS `dataset` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `filepath` varchar(128) NOT NULL,
  `type` varchar(32) NOT NULL,
  `dataset_ref` varchar(128) NOT NULL,
  `title` varchar(256) NOT NULL,
  `revision` varchar(64) NOT NULL,
  `data_url` varchar(2083) NOT NULL,
  `user_url` varchar(2083) NOT NULL,
  `creation_date` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='descriptif du jeu de donn�es' AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Structure de la table `geodata`
--

CREATE TABLE IF NOT EXISTS `geodata` (
  `latitude` double NOT NULL,
  `longitude` double NOT NULL,
  `dataset_id` int(11) NOT NULL,
  `creation_date` datetime,
  KEY `latitude` (`latitude`,`longitude`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- tables de regroupement pour optimissation
--
CREATE TABLE `geodata_group32` (
  `latitude` double NOT NULL,
  `longitude` double NOT NULL,
  `nb_points` int(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
SELECT * FROM odhm.geodata;


CREATE TABLE `geodata_group256` (
  `latitude` double NOT NULL,
  `longitude` double NOT NULL,
  `nb_points` int(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
SELECT * FROM odhm.geodata;

CREATE TABLE `geodata_group2048` (
  `latitude` double NOT NULL,
  `longitude` double NOT NULL,
  `nb_points` int(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
SELECT * FROM odhm.geodata;