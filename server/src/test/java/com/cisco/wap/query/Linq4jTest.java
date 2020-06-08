package com.cisco.wap.query;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.*;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

public class Linq4jTest {
    private List<Person> people = Lists.newArrayList();
    private List<Worker> workers = Lists.newArrayList();
    @Before
    public void init() throws IOException {
        URL url = Resources.getResource("data.txt");
        BufferedReader reader = new BufferedReader(new FileReader(url.getFile()));
        String currentLine = null;
        while((currentLine = reader.readLine()) != null){
            String[] split = currentLine.split(",");
            people.add(new Person(split[0], Integer.parseInt(split[1]), Boolean.parseBoolean(split[2])));
            workers.add(new Worker(split[0], Integer.parseInt(split[1]), Boolean.parseBoolean(split[2]), split[3]));
        }
    }

    @Test
    public void tryProjectOrder() {
        List<Person> results = Linq4j.asEnumerable(people).where(new Predicate1<Person>() {
            public boolean apply(Person arg0) {
                return arg0.sex;
            }
        }).select(new Function1<Person, Person>() {
            public Person apply(Person arg0) {
                return arg0;
            }
        }).orderByDescending(new Function1<Person, String>() {
            @Override
            public String apply(Person a0) {
                return a0.name;
            }
        }).toList();
        System.out.println(results);
    }

    @Test
    public void tryProjectOrderLambda() {
        List<Person> results = Linq4j.asEnumerable(people)
                .where(arg0 -> arg0.sex)
                .select(arg0 -> arg0)
                .orderByDescending(arg0 -> arg0.age)
                .toList();
        System.out.println(results);
    }

    @Test
    public void tryProjectPredicate() {
        final String[] where = {"Bob"};
        List<Person> results = Linq4j.asEnumerable(people)
                .where(new Predicate1<Person>() {
                    public boolean apply(Person arg0) {
                        return Linq4j.asEnumerable(where).contains(arg0.name);
                    }
                }).toList();
        System.out.println(results);
    }

    @Test
    public void tryPredicatesLambda(){
        List<Worker> workers = Linq4j.asEnumerable(this.workers).asQueryable().where(i ->
             i.age > 30 && "ibm".equals(i.company)
        ).toList();
        System.out.println(workers);
    }

    @Test
    public void trySelectPredicateLambda() {
        final String[] where = {"Bob", "Andy"};
        List<Person> results = Linq4j.asEnumerable(people)
                .where(i -> Linq4j.asEnumerable(where).contains(i.name))
                .toList();
        System.out.println(results);
    }

    @Test
    public void trySelectProjectsLambda() {
        List<Tuple2<String, Integer>> results = Linq4j.asEnumerable(people)
                .where(w -> w.sex)
                .select(s -> new Tuple2<>(s.name, s.age))
                .toList();
        System.out.println(results);
    }

    @Test
    public void tryConvert(){
        List<Person> persons = Linq4j.asEnumerable(workers)
                .ofType(Person.class).toList();
        System.out.println(persons);
    }

    @Test
    public void testGroupby() {
        List<Tuple2<String, Integer>> list = Linq4j.asEnumerable(workers)
                .groupBy(x -> x.company,
                        () -> 0,
                        (Function2<Integer, Worker, Integer>) (v, w) -> v+1,
                        (Function2<String, Integer, Tuple2<String, Integer>>) (k, v) -> new Tuple2<>(k, v)
                )
                .toList();
        list.stream().forEach(i -> System.out.println(i.k+": "+i.v));
    }

    @Test
    public void tryExpression() {
        /**
         * reference
         * https://github.com/julianhyde/linq4j/blob/master/src/test/java/net/hydromatic/linq4j/test/ExpressionTest.java
         */
        ParameterExpression parameterPerson = Expressions.parameter(Worker.class);
        ParameterExpression parameterInt = Expressions.parameter(Integer.TYPE);
        MemberExpression predicateName = Expressions.field(parameterPerson, "company");
        ConstantExpression constant = Expressions.constant("ibm");
        BinaryExpression filterOne = Expressions.equal(predicateName, constant);
        MemberExpression predicateAge = Expressions.field(parameterPerson, "age");
        ConstantExpression constant1 = Expressions.constant(30);
        BinaryExpression filterTwo = Expressions.greaterThan(predicateAge, constant1);
        BinaryExpression andAlso = Expressions.andAlso(filterOne, filterTwo);
        Queryable<Worker> results = Linq4j.asEnumerable(this.workers)
                .asQueryable()
                .whereN(
                        (FunctionExpression<? extends Predicate2<Worker, Integer>>) Expressions.lambda(
                                Predicate2.class,
                                andAlso,
                                parameterPerson,
                                parameterInt
                        )
                );
        Iterator<Worker> iterator = results.iterator();
        while(iterator.hasNext()) {
            Worker next = iterator.next();
            System.out.println(next);
        }
    }




    public class Person {
        public int age;
        public String name;
        public boolean sex;

        public Person(String name, int age, boolean sex) {
            this.name = name;
            this.age = age;
            this.sex = sex;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "age=" + age +
                    ", name='" + name + '\'' +
                    ", sex=" + sex +
                    '}';
        }
    }

    public class Worker extends Person {
        public String company;

        public Worker(String name, int age, boolean sex, String company) {
            super(name, age, sex);
            this.company = company;
        }
    }

    class Tuple2<K,V> {
        K k;
        V v;

        public Tuple2(K k, V v) {
            this.k = k;
            this.v = v;
        }

        @Override
        public String toString() {
            return "Tuple2{" +
                    "k=" + k +
                    ", v=" + v +
                    '}';
        }
    }
}
