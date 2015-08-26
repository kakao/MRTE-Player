package com.kakao.util;

import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public class CommandLineOption{
	final static String HELP_MSG_HEADER = "--- Command Usage ------------------------------------------------------" ;
	final static String HELP_MSG_FOOTER = "------------------------------------------------------------------------" ;
	
	final CommandLine cmdLine;
	final Options options;
	final CommandLineParser parser;
	
	public CommandLineOption(String[] args) throws Exception{
		try{
			this.options = generateCmdLineOptions();
			this.parser = new PosixParser();
			// this.parser = new GnuParser();
			this.cmdLine = parser.parse(options, args);
		}catch(Exception ex){
			throw ex;
		}
	}
	
	public boolean getBooleanParameter(String paramName, boolean defaultValue) throws IllegalArgumentException, NumberFormatException{
		if(!cmdLine.hasOption(paramName)){
			return defaultValue;
		}else{
			String temp = cmdLine.getOptionValue(paramName);
			if(temp==null || temp.trim().length()<=0){
				return defaultValue;
			}
			
			boolean boolValue = (temp.equalsIgnoreCase("yes") || temp.equalsIgnoreCase("true") || temp.equalsIgnoreCase("y") || temp.equalsIgnoreCase("on"));
			return boolValue;
		}
	}
	
	public long getLongParameter(String paramName) throws IllegalArgumentException, NumberFormatException{
		if(!cmdLine.hasOption(paramName)){
			throw new IllegalArgumentException("Option incorrect : parameter '"+paramName+"' is required");
		}else{
			String temp = cmdLine.getOptionValue(paramName);
			if(temp==null || temp.trim().length()<=0){
				throw new IllegalArgumentException("Option incorrect : parameter '"+paramName+"' is required");
			}
			long longValue = Long.parseLong(temp);
			return longValue;
		}
	}
	
	public long getLongParameter(String paramName, long defaultValue) throws IllegalArgumentException, NumberFormatException{
		if(!cmdLine.hasOption(paramName)){
			return defaultValue;
		}else{
			String temp = cmdLine.getOptionValue(paramName);
			if(temp==null || temp.trim().length()<=0){
				return defaultValue;
			}
			long longValue = Long.parseLong(temp);
			return longValue;
		}
	}
	
	public int getIntParameter(String paramName) throws IllegalArgumentException, NumberFormatException{
		if(!cmdLine.hasOption(paramName)){
			throw new IllegalArgumentException("Option incorrect : parameter '"+paramName+"' is required");
		}else{
			String temp = cmdLine.getOptionValue(paramName);
			if(temp==null || temp.trim().length()<=0){
				throw new IllegalArgumentException("Option incorrect : parameter '"+paramName+"' is required");
			}
			int intValue = Integer.parseInt(temp);
			return intValue;
		}
	}
	
	public int getIntParameter(String paramName, int defaultValue) throws IllegalArgumentException, NumberFormatException{
		if(!cmdLine.hasOption(paramName)){
			return defaultValue;
		}else{
			String temp = cmdLine.getOptionValue(paramName);
			if(temp==null || temp.trim().length()<=0){
				return defaultValue;
			}
			
			int intValue = Integer.parseInt(temp);
			return intValue;
		}
	}
	
	public double getDoubleParameter(String paramName, double defaultValue) throws IllegalArgumentException, NumberFormatException{
		if(!cmdLine.hasOption(paramName)){
			return defaultValue;
//			System.out.println("Option incorrect : parameter '"+paramName+"' is required");
//			HelpFormatter f = new HelpFormatter();
//			f.printHelp("Usage::", options);
//			throw new IllegalArgumentException("Option incorrect : parameter '"+paramName+"' is required");
		}else{
			String temp = cmdLine.getOptionValue(paramName);
			if(temp==null || temp.trim().length()<=0){
				return defaultValue;
				// throw new IllegalArgumentException("Option incorrect : parameter '"+paramName+"' is required");
			}
			
			try{
				double zipfianValue = Double.parseDouble(temp);
				return zipfianValue;
			}catch(Throwable t){
				return defaultValue;
			}
		}
	}
	
	public String getStringParameter(String paramName) throws IllegalArgumentException, NumberFormatException{
		if(!cmdLine.hasOption(paramName)){
			throw new IllegalArgumentException("Option incorrect : parameter '"+paramName+"' is required");
		}else{
			String value = cmdLine.getOptionValue(paramName);
			if(value==null || value.trim().length()<=0){
				throw new IllegalArgumentException("Option incorrect : parameter '"+paramName+"' is required");
			}
			return value;
		}
	}
	
	public String getStringParameter(String paramName, String defaultValue) throws IllegalArgumentException, NumberFormatException{
		if(!cmdLine.hasOption(paramName)){
			return defaultValue;
		}else{
			String value = cmdLine.getOptionValue(paramName);
			if(value==null || value.trim().length()<=0){
				return defaultValue;
			}
			return value;
		}
	}
	
	protected Options generateCmdLineOptions(){
		Options options = new Options();
		options.addOption("mh", "mysql_host", true, "MySQL target host");
		options.addOption("mP", "mysql_port", true, "MySQL port");
		options.addOption("mu", "mysql_user", true, "MySQL user");
		options.addOption("mp", "mysql_password", true, "MySQL password");

		options.addOption("mc", "mysql_init_conn", true, "Initial prepared connections");
		options.addOption("md", "mysql_default_db", true, "Default database");

		options.addOption("rh", "rabbitmq_host", true, "RabbitMQ target host");
		options.addOption("rP", "rabbitmq_port", true, "RabbitMQ port");
		options.addOption("ru", "rabbitmq_user", true, "RabbitMQ user");
		options.addOption("rp", "rabbitmq_password", true, "RabbitMQ password");
		options.addOption("qn", "rabbitmq_queue_name", true, "RabbitMQ queue name");
		options.addOption("rt", "rabbitmq_routing_key", true, "RabbitMQ routing key");
		
		options.addOption("sq", "slow_query_time", true, "Long query time in milli-seconds");
		// Added parameter let MRTE-Player recognize max_allowed_packet_size or source MySQL server. 
		options.addOption("mp", "max_packet_size", true, "Max allowed packet size of MySQL in bytes");
		
		options.addOption("so", "select_only", true, "Replay only select query");
		options.addOption("dr", "database_remap", true, "Database remapping options");
				
		return options;
	}

	public void printHelp(final int printedRowWidth, final int spacesBeforeOption, final int spacesBeforeOptionDescription, final boolean displayUsage, final OutputStream out) {
		final String commandLineSyntax = "java " + this.getClass().getCanonicalName();
		final PrintWriter writer = new PrintWriter(out);
		final HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.printHelp(writer, printedRowWidth, commandLineSyntax, HELP_MSG_HEADER, options, spacesBeforeOption, spacesBeforeOptionDescription, HELP_MSG_FOOTER, displayUsage);
		writer.flush();
	}
}