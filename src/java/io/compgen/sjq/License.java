package io.compgen.sjq;

import io.compgen.MainBuilder;
import io.compgen.annotation.Command;

import java.io.IOException;

@Command(name = "license", desc="Show the license", category="help")
public class License {
	public void exec() throws IOException {
		System.out.println(MainBuilder.readFile("LICENSE"));
	}
}

