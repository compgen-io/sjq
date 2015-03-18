package io.compgen.sjq;

import io.compgen.Exec;
import io.compgen.MainBuilder;
import io.compgen.annotation.Command;

@Command(name = "license", desc="Show the license", category="help")
public class License implements Exec{
	@Override
	public void exec() throws Exception {
		System.out.println(MainBuilder.readFile("LICENSE"));
	}
}

