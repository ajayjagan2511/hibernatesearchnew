package org.acme;


import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.hibernate.search.engine.search.query.SearchResult;
import org.hibernate.search.mapper.orm.session.SearchSession;
import org.jboss.resteasy.annotations.jaxrs.FormParam;
import org.jboss.resteasy.annotations.jaxrs.PathParam;
import org.jboss.resteasy.annotations.jaxrs.QueryParam;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.transaction.Transactional;
import javax.ws.rs.*;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static javax.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;

@Consumes(APPLICATION_FORM_URLENCODED)
@Path("/library")
@Tag(name="Library Resource",description = "Library REST APIs")
public class LibraryResource {

    @PUT
    @Path("book")
    @Transactional
    @Operation(
    operationId = "addBook" ,
	summary = "Add a New Book",
	description = "Insert a New Book into the Library if Author Exists"
    )
    public void addBook(
            @Parameter(
                    description = "Book Title",
                    required = true
            )
            @FormParam String title,
            @Parameter(
                    description = "Author Id",
                    required = true
            )
            @FormParam Long authorId) {
        Author author = Author.findById(authorId);
        if (author != null) {
            Book book = new Book();
            book.title = title;
            book.author = author;
            book.persist();

            author.books.add(book);
            author.persist();
        }
    }


    @PUT
    @Path("author")
    @Transactional
    @Operation(
            operationId = "addAuthor" ,
            summary = "Add a New Author",
            description = "Insert a New Author into the Library"
    )
    public void addAuthor(
            @Parameter(
                    description = "Author First name",
                    required = true
            )
            @FormParam String firstName,
            @Parameter(
                    description = "Author Last name",
                    required = true
            )
            @FormParam String lastName) {
        Author author = new Author();
        author.firstName = firstName;
        author.lastName = lastName;
        author.persist();
    }

    @POST
    @Path("author/{id}")
    @Transactional
    @Operation(
            operationId = "updateAuthor" ,
            summary = "update Author",
            description = "update author firstName and lastName"
    )
    public void updateAuthor(
            @Parameter(
                    description = "Author id",
                    required = true
            )
            @PathParam Long id,
            @Parameter(
                     description = "Author First name",
                     required = true
             )
             @FormParam String firstName,
             @Parameter(
                     description = "Author Last name",
                     required = true
             )
             @FormParam String lastName) {
        Author author = Author.findById(id);
        if (author != null) {
            author.firstName = firstName;
            author.lastName = lastName;
            author.persist();
        }
    }

    @DELETE
    @Path("book/{id}")
    @Transactional
    @Operation(
            operationId = "deleteBook" ,
            summary = "Delete a book",
            description = "Delete a book"
    )
    public void deleteBook(
            @Parameter(
                    description = "author id",
                    required = true
            )
            @PathParam Long id) {
        Book book = Book.findById(id);
        if (book != null) {
            book.author.books.remove(book);
            book.delete();
        }
    }

    @DELETE
    @Path("author/{id}")
    @Transactional
    @Operation(
            operationId = "deleteAuthor" ,
            summary = "Delete an Author",
            description = "Delete an author and all his books"
    )
    public void deleteAuthor(
            @Parameter(
                    description = "Author id",
                    required = true
            )
            @PathParam Long id) {
        Author author = Author.findById(id);
        if (author != null) {
            author.delete();
        }
    }

    @Inject
    SearchSession searchSession;

    @Transactional
    void onStart(@Observes StartupEvent ev) throws InterruptedException {
        // only reindex if we imported some content
        if (Book.count() > 0) {
            searchSession.massIndexer()
                    .startAndWait();
        }
    }

    @GET
    @Path("authors")
    @Transactional
    @Operation(
            operationId = "searchAuthors" ,
            summary = "Find all Authors",
            description = "Get all the information of all authors in database"
    )
    public List<Author> searchAuthors() {
        return searchSession.search(Author.class)
                .where(f -> f.matchAll())
                .sort(f -> f.field("lastName_sort").then().field("firstName_sort"))
                .fetchAllHits();
    }

    @GET
    @Path("authors/time")
    @Transactional
    @Operation(
            operationId = "searchAuthorsTime" ,
            summary = "Time to Find all Authors",
            description = "Get Time to find all authors in database"
    )
    public Duration searchAuthorsTime() {
        SearchResult<Author> result= searchSession.search(Author.class)
                .where(f -> f.matchAll())
                .sort(f -> f.field("lastName_sort").then().field("firstName_sort"))
                .fetchAll();
        return result.took();

    }

    @GET
    @Path("author/{id}")
    @Transactional
    @Operation(
            operationId = "searchAuthors" ,
            summary = "Find Author by id",
            description = "Get the information of author in database"
    )
    public List<Author> searchAuthors(
            @Parameter(
                    description = "Author Id",
                    required = true
            )
            @org.jboss.resteasy.annotations.jaxrs.PathParam long id) {
        return searchSession.search(Author.class)
                .where(f -> f.id().matching(id)).fetchAllHits();
    }

    @GET
    @Path("author/search")
    @Transactional
    @Operation(
            operationId = "searchAuthors" ,
            summary = "Find Author by First/Last Name/Book Tile",
            description = "Get Author by first/last name, where even if first 3 letters match, it will be accepted, or partial book title"
    )
    public List<Author> searchAuthors(
            @Parameter(
                    description = "First/Last Name/Book Tile",
                    required = true
            )
            @org.jboss.resteasy.annotations.jaxrs.QueryParam String pattern,
            @Parameter(
                    description = "size",
                    required = false
            )
            @QueryParam Optional<Integer> size) {
        return searchSession.search(Author.class)
                .where(f ->
                                f.simpleQueryString()
                                        .fields("firstName", "lastName", "books.title").matching(pattern)
                )
                .sort(f -> f.field("lastName_sort").then().field("firstName_sort"))
                .fetchAllHits();
    }

    @GET
    @Path("author/time/search")
    @Transactional
    @Operation(
            operationId = "searchAuthorsTime" ,
            summary = "Find time to find Author by First/Last Name/Book Tile",
            description = "Get time to find Author by first/last name, where even if first 3 letters match, it will be accepted, or partial book title"
    )
    public Duration searchAuthorsTime(@Parameter(
            description = "First/Last Name/Book Tile",
            required = true
            )
            @QueryParam String pattern,
          @Parameter(
                  description = "size",
                  required = false
          )
          @QueryParam Optional<Integer> size) {
        SearchResult<Author> result= searchSession.search(Author.class)
                .where(f ->
                                f.simpleQueryString()
                                        .fields("firstName", "lastName","books.title").matching(pattern)
                )
                .sort(f -> f.field("lastName_sort").then().field("firstName_sort"))
                .fetchAll();
        return result.took();
    }

    @GET
    @Path("books")
    @Transactional
    @Operation(
            operationId = "searchBook" ,
            summary = "Get all Books",
            description = "Get all Books in the databsse"
    )
    public List<Book> searchBook() {
        return searchSession.search(Book.class)
                .where(f -> f.matchAll())
                .sort(f->f.field("title_sort"))
                .fetchAllHits();
    }

    @GET
    @Path("books/time")
    @Transactional
    @Operation(
            operationId = "searchBookTime" ,
            summary = "time to get all Books",
            description = "Get time to fetch all Books in the databsse"
    )
    public Duration searchBookTime() {
        SearchResult<Book> result= searchSession.search(Book.class)
                .where(f -> f.matchAll())
                .sort(f->f.field("title_sort"))
                .fetchAll();
        return result.took();
    }

    @GET
    @Path("books/page")
    @Transactional
    @Operation(
            operationId = "searchBookPage" ,
            summary = "Get all Books in a page",
            description = "Get all Books in page by defining page and no of books in a page"
    )
    public List<Book> searchBookPage(@Parameter(
            description = "page no",
            required = true
            )
            @org.jboss.resteasy.annotations.jaxrs.QueryParam int page,
             @Parameter(
                     description = "books in a page",
                     required = true
             )
             @QueryParam int limit) {
        return searchSession.search(Book.class)
                .where(f -> f.matchAll())
                .sort(f->f.field("title_sort"))
                .fetchHits(limit*(page-1),limit);
    }

    @GET
    @Path("book/{id}")
    @Transactional
    @Operation(
            operationId = "searchBookId" ,
            summary = "Get Book by id",
            description = "Get Book from database by giving Book.id"
    )
    public List<Book> searchBookId(
            @Parameter(
                    description = "book id",
                    required = true
            )
            @PathParam long id) {
        return searchSession.search(Book.class)
                .where(f -> f.id().matching(id))
                .fetchAllHits();
    }

    @GET
    @Path("book/search")
    @Transactional
    @Operation(
            operationId = "searchBook" ,
            summary = "Search Books by Text-Search",
            description = "Perform text-search on book.title to get relevant books "
    )
    public List<Book> searchBook(
            @Parameter(
                    description = "Pattern",
                    required = true
            )
            @org.jboss.resteasy.annotations.jaxrs.QueryParam String pattern,
            @Parameter(
                    description = "size",
                    required = false
            )
            @QueryParam Optional<Integer> size) {
        return searchSession.search(Book.class)
                .where(f ->
                                f.simpleQueryString()
                                        .fields("title").matching(pattern)
                )
                .sort(f->f.score().desc())
                .fetchAllHits();
    }

    @GET
    @Path("book/time/search")
    @Transactional
    @Operation(
            operationId = "searchBookTime" ,
            summary = "Time to Search Books by Text-Search",
            description = "Time to Perform text-search on book.title to get relevant books "
    )
    public Duration searchBookTime(
            @Parameter(
                    description = "Pattern",
                    required = true
            )
            @org.jboss.resteasy.annotations.jaxrs.QueryParam String pattern,
            @Parameter(
                    description = "size",
                    required = false
            )
            @QueryParam Optional<Integer> size) {
        SearchResult<Book> result= searchSession.search(Book.class)
                .where(f ->
                                f.simpleQueryString()
                                        .fields("title").matching(pattern)
                )
                .sort(f->f.score().desc())
                .fetchAll();
        return result.took();
    }

    @GET
    @Path("book/wildcard/search")
    @Transactional
    @Operation(
            operationId = "searchBookWildcard" ,
            summary = "Search Books by Text-Search with partial words",
            description = "Perform text-search on book.title to get relevant books using wildcard words (similar to like in SQL) "
    )
    public List<Book> searchBookWildcard(
            @Parameter(
                    description = "Pattern",
                    required = true
            )
            @org.jboss.resteasy.annotations.jaxrs.QueryParam String pattern,
            @Parameter(
                    description = "size",
                    required = false
            )
            @QueryParam Optional<Integer> size) {
        return searchSession.search(Book.class)
                .where(f->
                                f.wildcard()
                                        .fields("title").matching(pattern)
                )
                .sort(f->f.score().desc())
                .fetchAllHits();
    }

    @GET
    @Path("book/phrase/slop/search")
    @Transactional
    @Operation(
            operationId = "searchBookPhraseSlop" ,
            summary = "Search Books by phrase-Search with 2 permissible words within phrase",
            description = "Perform phrase-search on book.title to get relevant books using f.phrase where maximum of 2 slop words are allowed "
    )
    public List<Book> searchBookPhraseSlop(
            @Parameter(
                    description = "Pattern",
                    required = true
            )
            @org.jboss.resteasy.annotations.jaxrs.QueryParam String pattern,
            @Parameter(
                    description = "size",
                    required = false
            )
            @QueryParam Optional<Integer> size) {
        return searchSession.search(Book.class)
                .where(f ->
                                f.phrase()
                                        .fields("title").matching(pattern).slop(2)
                )
                .sort(f->f.score().desc())
                .fetchAllHits();
    }

    @GET
    @Path("book/except/search")
    @Transactional
    @Operation(
            operationId = "searchBookExcept" ,
            summary = "Search Books where these certain words dont exist",
            description = "Perform text-search on book.title to get relevant books where certain words must not be permitted "
    )
    public List<Book> searchBookExcept(
            @Parameter(
                    description = "Pattern",
                    required = true
            )
            @org.jboss.resteasy.annotations.jaxrs.QueryParam String pattern,
            @Parameter(
                    description = "size",
                    required = false
            )
            @QueryParam Optional<Integer> size) {
        return searchSession.search(Book.class)
                .where(f ->
                                f.matchAll()
                                        .except( f.match().fields("title").matching(pattern))
                )
                .sort(f->f.score().desc())
                .fetchAllHits();
    }

    @GET
    @Path("book/fuzzy/search")
    @Transactional
    @Operation(
            operationId = "searchBookFuzzy" ,
            summary = "Search Books by phrase-Search with 1 permissible character change in a word",
            description = "Perform phrase-search on book.title to get relevant books with 1 permissible character change in a word allowed (after the first 3 characters) "
    )
    public List<Book> searchBookFuzzy(
            @Parameter(
                    description = "Pattern",
                    required = true
            )
            @org.jboss.resteasy.annotations.jaxrs.QueryParam String pattern,
            @Parameter(
                    description = "size",
                    required = false
            )
            @QueryParam Optional<Integer> size) {
        return searchSession.search(Book.class)
                .where(f ->
                                f.match().fields("title").matching(pattern).fuzzy(1,3)
                )
                .sort(f->f.score().desc())
                .fetchAllHits();
    }

}
