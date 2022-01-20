package app

type Config struct {
	KSQL struct {
		URL      string `yaml:"url"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
	} `yaml:"ksql"`
	Connect struct {
		URL      string `yaml:"url"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
	} `yaml:"connect"`
}
