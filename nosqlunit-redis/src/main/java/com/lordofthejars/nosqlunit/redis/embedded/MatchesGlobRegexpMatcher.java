package com.lordofthejars.nosqlunit.redis.embedded;

import java.util.regex.Pattern;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class MatchesGlobRegexpMatcher extends TypeSafeMatcher<String> {

	protected final String regex;
	protected final Pattern compiledRegex;

	public MatchesGlobRegexpMatcher(final String regex) {
		this.regex = convertGlobToRegEx(regex);
		compiledRegex = Pattern.compile(this.regex);
	}

	private String convertGlobToRegEx(String line) {
		line = line.trim();
		int strLen = line.length();
		StringBuilder sb = new StringBuilder(strLen);
		// Remove beginning and ending * globs because they're useless
		if (line.startsWith("*")) {
			line = line.substring(1);
			strLen--;
		}
		if (line.endsWith("*")) {
			line = line.substring(0, strLen - 1);
			strLen--;
		}
		boolean escaping = false;
		int inCurlies = 0;
		for (char currentChar : line.toCharArray()) {
			switch (currentChar) {
			case '*':
				if (escaping)
					sb.append("\\*");
				else
					sb.append(".*");
				escaping = false;
				break;
			case '?':
				if (escaping)
					sb.append("\\?");
				else
					sb.append('.');
				escaping = false;
				break;
			case '.':
			case '(':
			case ')':
			case '+':
			case '|':
			case '^':
			case '$':
			case '@':
			case '%':
				sb.append('\\');
				sb.append(currentChar);
				escaping = false;
				break;
			case '\\':
				if (escaping) {
					sb.append("\\\\");
					escaping = false;
				} else
					escaping = true;
				break;
			case '{':
				if (escaping) {
					sb.append("\\{");
				} else {
					sb.append('(');
					inCurlies++;
				}
				escaping = false;
				break;
			case '}':
				if (inCurlies > 0 && !escaping) {
					sb.append(')');
					inCurlies--;
				} else if (escaping)
					sb.append("\\}");
				else
					sb.append("}");
				escaping = false;
				break;
			case ',':
				if (inCurlies > 0 && !escaping) {
					sb.append('|');
				} else if (escaping)
					sb.append("\\,");
				else
					sb.append(",");
				break;
			default:
				escaping = false;
				sb.append(currentChar);
			}
		}
		return sb.toString();
	}

	@Override
	public void describeTo(Description description) {
		description.appendText("matches regex ").appendValue(regex);
	}

	@Override
	protected boolean matchesSafely(String item) {
		return compiledRegex.matcher(item).matches();
	}

	public static Matcher<String> matches(final String regex) {
		return new MatchesGlobRegexpMatcher(regex);
	}

}
