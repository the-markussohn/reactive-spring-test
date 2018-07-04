package com.example.flixfluxservice;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Stream;

@SpringBootApplication
public class FlixFluxServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlixFluxServiceApplication.class, args);
    }
}

@Component
class MovieCLR implements CommandLineRunner {

    private final MovieRepository movieRepository;

    public MovieCLR(MovieRepository movieRepository) {
        this.movieRepository = movieRepository;
    }

    @Override
    public void run(String... args) throws Exception {

        Flux<Movie> movies = movieRepository.deleteAll().thenMany(
                Flux.just("Silence of the Lambdas", "AEon Flux", "Enter the Mono<Void>",
                        "Back to the Future", "Meet the Fluxxes")
                        .map(title -> new Movie(title, UUID.randomUUID().toString()))
                        .flatMap(movieRepository::save));

        movies.subscribe(null, null, () -> {
            movieRepository.findAll().subscribe(System.out::println);
        });
    }
}

@RestController
@RequestMapping("/movies")
class MovieRestController {

    private final MovieService movieService;

    public MovieRestController(MovieService movieService) {
        this.movieService = movieService;
    }

    @GetMapping
    public Flux<Movie> getAll() {
        return movieService.all();
    }

    @GetMapping("/{movieId}")
    public Mono<Movie> byId(@PathVariable String movieId) {
        return movieService.byId(movieId);
    }

    @GetMapping(path = "/{movieId}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<MovieEvent> events(@PathVariable String movieId) {
        return movieService.events(movieId);
    }
}

@Service
class MovieService {

    private final MovieRepository movieRepository;

    public MovieService(MovieRepository movieRepository) {
        this.movieRepository = movieRepository;
    }

    public Flux<Movie> all() {
        return movieRepository.findAll();
    }

    public Mono<Movie> byId(String movieId) {
        return movieRepository.findById(movieId);
    }

    public Flux<MovieEvent> events(String movieId) {

        Mono<Movie> mono = byId(movieId);

        return mono.flatMapMany(movie -> {
            Flux<Long> interval = Flux.interval(Duration.ofSeconds(1));
            Flux<MovieEvent> flux = Flux.fromStream(Stream.generate(() -> new MovieEvent(movie, new Date())));

            Flux<Tuple2<Long, MovieEvent>> tuple2Flux = Flux.zip(interval, flux);

            return tuple2Flux.map(Tuple2::getT2);
        });
    }

}

@Data
@NoArgsConstructor
@AllArgsConstructor
class MovieEvent {
    private Movie movie;
    private Date date;
}

interface MovieRepository extends ReactiveMongoRepository<Movie, String> {
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Document
class Movie {
    private String title;
    @Id
    private String id;
}