package lk.ijse.dep9.api;

import jakarta.annotation.Resource;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lk.ijse.dep9.api.util.HttpServlet2;
import lk.ijse.dep9.dto.IssueNoteDTO;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.util.stream.Collectors;

@WebServlet(name = "IssueNoteServlet", value = "/issue-notes/*")
public class IssueNoteServlet extends HttpServlet2 {

    @Resource(lookup = "java:comp/env/jdbc/dep9-lms")
    private DataSource pool;

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (request.getPathInfo() != null && !request.getPathInfo().equals("/")) {
            response.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED);
            return;
        }

        try {
            if (request.getContentType() == null || !request.getContentType().startsWith("application/json")) {
                throw new JsonbException("Invalid JSON");
            }

            IssueNoteDTO issueNote = JsonbBuilder.create().fromJson(request.getReader(), IssueNoteDTO.class);
            createNewIssueNote(issueNote, response);
        } catch (JsonbException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
        }
    }

    private void createNewIssueNote(IssueNoteDTO issueNoteDTO, HttpServletResponse response) throws IOException {
        /* Data Validation */
        if (issueNoteDTO.getMemberId() == null ||
                !issueNoteDTO.getMemberId().matches("([A-Fa-f0-9]{8}(-[A-Fa-f0-9]{4}){3}-[A-Fa-f0-9]{12})")) {
            throw new JsonbException("The member id is empty or invalid");
        } else if (issueNoteDTO.getBooks().isEmpty()) {
            throw new JsonbException("A issue note requires at least one book");
        } else if (issueNoteDTO.getBooks().size() > 3) {
            throw new JsonbException("A member can't borrow more than 3 books");
        } else if (!issueNoteDTO.getBooks().stream().
                allMatch(isbn -> isbn.matches("([0-9][0-9\\\\-]*[0-9])"))) {
            throw new JsonbException("Invalid isbn has been found");
        } else if (issueNoteDTO.getBooks().stream().collect(Collectors.toSet()).size() !=
                issueNoteDTO.getBooks().size()) {
            throw new JsonbException("Duplicate isbn has been found");
        }

        /* Business Validation */
        try (Connection connection = pool.getConnection()) {
            PreparedStatement stmExist = connection.prepareStatement("SELECT id FROM member WHERE id=?");
            stmExist.setString(1, issueNoteDTO.getMemberId());
            if (!stmExist.executeQuery().next()) {
                throw new JsonbException("Member does not exist within the database");
            }

            PreparedStatement stm = connection.prepareStatement(
                    "SELECT b.title, ((b.copies - COUNT(issue_item.isbn)) > 0) as `availability` FROM issue_item " +
                            "    LEFT OUTER JOIN `return` r ON issue_item.issue_id = r.issue_id and issue_item.isbn = r.isbn " +
                            "    RIGHT OUTER JOIN book b on issue_item.isbn = b.isbn " +
                            "    WHERE r.date IS NULL and b.isbn = ? GROUP BY b.isbn");

            PreparedStatement stmDuplicateExist = connection.prepareStatement(
                    "SELECT name, ii.isbn " +
                            "FROM member " +
                            "         INNER JOIN issue_note `in` ON member.id = `in`.member_id " +
                            "         INNER JOIN issue_item ii ON `in`.id = ii.issue_id " +
                            "         LEFT OUTER JOIN `return` r ON ii.issue_id = r.issue_id and ii.isbn = r.isbn " +
                            "WHERE r.date IS NULL AND member.id = ? AND ii.isbn = ?"
            );
            stmDuplicateExist.setString(1, issueNoteDTO.getMemberId());

            for (String isbn : issueNoteDTO.getBooks()) {
                stm.setString(1, isbn);
                stmDuplicateExist.setString(2, isbn);
                ResultSet rst = stm.executeQuery();
                ResultSet rst2 = stmDuplicateExist.executeQuery();
                if (!rst.next()) throw new JsonbException(isbn + " book doesn't exist within the database");
                if (!rst.getBoolean("availability")) {
                    throw new JsonbException(isbn + " book is not available at the moment");
                }
                if (rst2.next()) throw new JsonbException(isbn + " book has been already issued to the same member");
            }

            PreparedStatement stm2 = connection.prepareStatement(
                    "SELECT member.id, name, 3 - COUNT(`in`.id) as `available` FROM member " +
                            "LEFT OUTER JOIN issue_note `in` ON member.id = `in`.member_id " +
                            "LEFT OUTER JOIN issue_item ii ON `in`.id = ii.issue_id " +
                            "LEFT OUTER JOIN `return` r ON ii.issue_id = r.issue_id and ii.isbn = r.isbn " +
                            "WHERE r.date IS NULL AND member.id = ? GROUP BY member.id"
            );
            stm2.setString(1, issueNoteDTO.getMemberId());
            ResultSet rst = stm2.executeQuery();
            rst.next();
            int available = rst.getInt("available");
            if (available < issueNoteDTO.getBooks().size()){
                throw new JsonbException("Issue limit is exceeded, only " + available + " books are available");
            }

            try {
                connection.setAutoCommit(false);

                PreparedStatement stmIssueNote =
                        connection.prepareStatement("INSERT INTO issue_note (date, member_id) VALUES (?, ?)",
                                Statement.RETURN_GENERATED_KEYS);
                stmIssueNote.setDate(1, Date.valueOf(LocalDate.now()));
                stmIssueNote.setString(2, issueNoteDTO.getMemberId());
                if (stmIssueNote.executeUpdate() != 1) throw new SQLException("Failed to insert the issue note");

                ResultSet generatedKeys = stmIssueNote.getGeneratedKeys();
                generatedKeys.next();
                int issueNoteId = generatedKeys.getInt(1);

                PreparedStatement stmIssueItem =
                        connection.prepareStatement("INSERT INTO issue_item (issue_id, isbn) VALUES (?, ?)");
                stmIssueItem.setInt(1, issueNoteId);
                for (String isbn : issueNoteDTO.getBooks()) {
                    stmIssueItem.setString(2, isbn);
                    if (stmIssueItem.executeUpdate() != 1) throw new SQLException("Failed to insert the issue item");
                }

                connection.commit();

                issueNoteDTO.setDate(LocalDate.now());
                issueNoteDTO.setId(issueNoteId);
                response.setStatus(HttpServletResponse.SC_CREATED);
                response.setContentType("application/json");
                JsonbBuilder.create().toJson(issueNoteDTO, response.getWriter());

            }catch (Throwable t){
                connection.rollback();
                t.printStackTrace();
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed to place the issue note");
            }finally{
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed to place the issue note");
        }
    }
}
