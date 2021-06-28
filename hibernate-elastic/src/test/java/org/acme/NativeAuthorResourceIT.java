package org.acme;

import io.quarkus.test.junit.NativeImageTest;

@NativeImageTest
public class NativeAuthorResourceIT extends AuthorResourceTest {

    // Execute the same tests but in native mode.
}