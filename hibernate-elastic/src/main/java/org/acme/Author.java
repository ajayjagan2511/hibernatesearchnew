package org.acme;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import org.hibernate.search.engine.backend.types.Sortable;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.FullTextField;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.Indexed;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.IndexedEmbedded;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.KeywordField;

import javax.persistence.*;
import java.util.List;

@Entity
@Indexed
@Table(name="Author")
public class Author extends PanacheEntity {

    @FullTextField(analyzer = "name")
    @KeywordField(name = "firstName_sort", sortable = Sortable.YES, normalizer = "sort")
    @Column(length=100)
    public String firstName;

    @FullTextField(analyzer = "name")
    @KeywordField(name = "lastName_sort", sortable = Sortable.YES, normalizer = "sort")
    @Column(length=100)
    public String lastName;

    @OneToMany(mappedBy = "author", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    @IndexedEmbedded
    public List<Book> books;



}
