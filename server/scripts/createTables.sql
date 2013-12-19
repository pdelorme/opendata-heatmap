DROP TABLE dataset;
DROP TABLE geodata;

--
-- Structure de la table `dataset`
--

CREATE TABLE IF NOT EXISTS `dataset` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL,
  `creation_date` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='descriptif du jeu de données' AUTO_INCREMENT=1 ;

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