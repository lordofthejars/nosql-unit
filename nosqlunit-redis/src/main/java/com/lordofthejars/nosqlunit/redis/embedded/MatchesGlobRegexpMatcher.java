package com.lordofthejars.nosqlunit.redis.embedded;

import java.util.regex.Pattern;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class MatchesGlobRegexpMatcher extends TypeSafeMatcher<String> {

	protected final String regex;

	public MatchesGlobRegexpMatcher(final String regex) {
		this.regex = regex;
	}

	private boolean match(String pattern, String string) {
		return match(pattern, string, null);
	}

	private boolean match(String pattern, String string, String[] substr) {
		return match(pattern, 0, string, 0, substr, 0);
	}

	private boolean match(String pat, int pIndex, String str, int sIndex, String[] substrs, int subIndex) {
		int pLen = pat.length();
		int sLen = str.length();

		while (true) {
			if (pIndex == pLen) {
				if (sIndex == sLen) {
					return true;
				} else {
					return false;
				}
			} else if ((sIndex == sLen) && (pat.charAt(pIndex) != '*')) {
				return false;
			}

			switch (pat.charAt(pIndex)) {
			case '*': {
				int start = sIndex;
				pIndex++;
				if (pIndex >= pLen) {
					addMatch(str, start, sLen, substrs, subIndex);
					return true;
				}
				while (true) {
					if (match(pat, pIndex, str, sIndex, substrs, subIndex + 1)) {
						addMatch(str, start, sIndex, substrs, subIndex);
						return true;
					}
					if (sIndex == sLen) {
						return false;
					}
					sIndex++;
				}
			}
			case '?': {
				pIndex++;
				addMatch(str, sIndex, sIndex + 1, substrs, subIndex++);
				sIndex++;
				break;
			}
			case '[': {
				try {
					pIndex++;
					char s = str.charAt(sIndex);
					char p = pat.charAt(pIndex);

					while (true) {
						if (p == ']') {
							return false;
						}
						if (p == s) {
							break;
						}
						pIndex++;
						char next = pat.charAt(pIndex);
						if (next == '-') {
							pIndex++;
							char p2 = pat.charAt(pIndex);
							if ((p <= s) && (s <= p2)) {
								break;
							}
							pIndex++;
							next = pat.charAt(pIndex);
						}
						p = next;
					}
					pIndex = pat.indexOf(']', pIndex) + 1;
					if (pIndex <= 0) {
						return false;
					}
					addMatch(str, sIndex, sIndex + 1, substrs, subIndex++);
					sIndex++;
				} catch (StringIndexOutOfBoundsException e) {
					/*
					 * Easier just to catch malformed [] sequences than to check
					 * bounds all the time.
					 */

					return false;
				}
				break;
			}
			case '\\': {
				pIndex++;
				if (pIndex >= pLen) {
					return false;
				}
				// fall through
			}
			default: {
				if (pat.charAt(pIndex) != str.charAt(sIndex)) {
					return false;
				}
				pIndex++;
				sIndex++;
			}
			}
		}
	}

	private void addMatch(String str, int start, int end, String[] substrs, int subIndex) {
		if ((substrs == null) || (subIndex >= substrs.length)) {
			return;
		}

		substrs[subIndex] = str.substring(start, end);
	}

	@Override
	public void describeTo(Description description) {
		description.appendText("matches regex ").appendValue(regex);
	}

	@Override
	protected boolean matchesSafely(String item) {
		return this.match(this.regex, item);
	}

	public static Matcher<String> matches(final String regex) {
		return new MatchesGlobRegexpMatcher(regex);
	}

}
