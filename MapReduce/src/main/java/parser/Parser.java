package parser;

public class Parser {
	private final int TICKER = 0;
	private final int NAME = 2;
	private final int EXCHANGE = 1;
	
	public Parser() {
	};

	public String processString(String text) {

		String[] fields = text.split(",");
		String ticker = fields[TICKER];
		String sector = fields[fields.length - 2];
		String industry = fields[fields.length - 1];
		String result = null;
		String name= fields[NAME];
		String exchange = fields[EXCHANGE];

		if (industry.equals("N/A") || exchange.equals("N/A") || ticker.equals("N/A") || sector.equals("N/A")|| name.equals("N/A")) {

			return result;
		}

		

		if (industry.indexOf('"') != -1) {

			sector = fields[fields.length - 3];
		}


		if (name.indexOf('"') != -1) {
			fields[2] = fields[2].replace('"', ' ');
			fields[3] = fields[3].replace('"', ' ');
			name = fields[2] + fields[3];

		}

		return result = ticker + "," + name + "," + sector;

	}

}
